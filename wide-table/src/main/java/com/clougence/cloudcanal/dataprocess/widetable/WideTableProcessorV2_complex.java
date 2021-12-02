package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class WideTableProcessorV2_complex implements CloudCanalProcessorV2 {

    private Map<SchemaInfo, Map<SchemaInfo, Map<String, String>>> joinKeys   = new HashMap<>();

    private Map<SchemaInfo, Map<SchemaInfo, List<CustomFieldV2>>> addCols    = new HashMap<>();

    private DataSource                                            srcDataSource;

    private static String                                         leftQuota  = "`";

    private static String                                         rightQuota = "`";

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();

        SchemaInfo workerTable = new SchemaInfo(null, "wide_table", "worker");
        Map<String, String> workerJoinKey = new LinkedHashMap<>();
        workerJoinKey.put("worker_id", "id");

        Map<SchemaInfo, Map<String, String>> dimensionJoinKeys = new HashMap<>();
        dimensionJoinKeys.put(workerTable, workerJoinKey);
        // ...can add more dimension table

        SchemaInfo factTable = new SchemaInfo(null, "wide_table", "task_schedule");
        joinKeys.put(factTable, dimensionJoinKeys);
        // ...can add more fact table

        Map<SchemaInfo, List<CustomFieldV2>> dimensionAddCols = new HashMap<>();
        List<CustomFieldV2> workerAddCols = new ArrayList<>();
        workerAddCols.add(CustomFieldV2.buildField("worker_state", null, Types.VARCHAR, false, false, true));
        workerAddCols.add(CustomFieldV2.buildField("worker_name", null, Types.VARCHAR, false, false, true));
        dimensionAddCols.put(workerTable, workerAddCols);

        addCols.put(factTable, dimensionAddCols);
    }

    protected String genPkInSql(SchemaInfo table, List<CustomFieldV2> addCols, List<CustomFieldV2> joinKeys, int group) {
        StringBuilder sb = new StringBuilder("SELECT ");

        boolean first = true;
        for (CustomFieldV2 j : joinKeys) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(leftQuota).append(j.getFieldName()).append(rightQuota);
        }

        for (CustomFieldV2 f : addCols) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(leftQuota).append(f.getFieldName()).append(rightQuota);
        }

        sb.append(" FROM ");

        if (StringUtils.isNotBlank(table.getCatalog())) {
            sb.append(leftQuota).append(table.getCatalog()).append(rightQuota).append(".");
        }

        sb.append(leftQuota).append(table.getSchema()).append(rightQuota).append(".");
        sb.append(leftQuota).append(table.getTable()).append(rightQuota);
        sb.append(" WHERE (");
        first = true;
        for (CustomFieldV2 joinKey : joinKeys) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(leftQuota).append(joinKey.getFieldName()).append(rightQuota);
        }

        sb.append(" ) IN (");

        for (int i = 0; i < group; i++) {
            if (i != 0) {
                sb.append(",");
            }

            sb.append("(");
            first = true;
            for (CustomFieldV2 joinKey : joinKeys) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }

                sb.append(leftQuota).append(joinKey.getFieldName()).append(rightQuota);
            }
            sb.append(")");
        }
        sb.append(")");

        return sb.toString();
    }

    protected CustomFieldV2 findCol(List<CustomFieldV2> fields, String targetFieldName) {
        for (CustomFieldV2 field : fields) {
            if (field.getFieldName().equals(targetFieldName)) {
                return field;
            }
        }

        return null;
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        // handleFactTable(re, data);
        // handleDimensionTable(re, data);
        re.add(data);
        return re;
    }

    protected void handleDimensionTable(List<CustomData> re, CustomData data) {
        // TODO
    }

    protected void handleFactTable(List<CustomData> re, CustomData data) {
        Map<SchemaInfo, Map<String, String>> dimensionJoinKeys = joinKeys.get(data.getSchemaInfo());
        if (dimensionJoinKeys == null || dimensionJoinKeys.isEmpty()) {
            re.add(data);
            return;
        }

        Map<SchemaInfo, List<CustomFieldV2>> dimensionAddCols = addCols.get(data.getSchemaInfo());

        for (Map.Entry<SchemaInfo, Map<String, String>> entry : dimensionJoinKeys.entrySet()) {
            List<List<CustomFieldV2>> dimensionKeys = new ArrayList<>();
            List<CustomRecordV2WithJoinKeys> recordV2WithJoinKeys = new ArrayList<>();
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                List<CustomFieldV2> recordKeys = new ArrayList<>();
                List<String> joinKeyVals = new ArrayList<>();
                for (Map.Entry<String, String> keyMap : entry.getValue().entrySet()) {
                    CustomFieldV2 f;
                    switch (data.getEventType()) {
                        case INSERT:
                        case UPDATE:
                            f = findCol(recordV2.getAfterColumns(), keyMap.getKey());
                            break;
                        case DELETE:
                            f = findCol(recordV2.getBeforeColumns(), keyMap.getKey());
                            break;
                        default:
                            throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
                    }

                    if (f == null) {
                        throw new IllegalArgumentException("join key not in source record (after image)! key:" + keyMap.getKey());
                    }

                    joinKeyVals.add(String.valueOf(f.getValue()));
                    recordKeys.add(CustomFieldV2.buildField(keyMap.getValue(), f.getValue(), f.getSqlType(), f.isKey(), f.isNull(), f.isUpdated()));
                }

                dimensionKeys.add(recordKeys);

                CustomRecordV2WithJoinKeys rjk = new CustomRecordV2WithJoinKeys(recordV2, joinKeyVals);
                recordV2WithJoinKeys.add(rjk);
            }

            List<CustomFieldV2> dAddCols = dimensionAddCols.get(entry.getKey());
            List<CustomFieldV2> sampleJoinKeys = dimensionKeys.get(0);
            String sql = genPkInSql(entry.getKey(), dAddCols, sampleJoinKeys, dimensionKeys.size());
            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
                int i = 1;
                for (List<CustomFieldV2> keys : dimensionKeys) {
                    for (CustomFieldV2 key : keys) {
                        ps.setObject(i, key.getValue(), key.getSqlType());
                        i++;
                    }
                }

                Map<List<String>, List<CustomFieldV2>> queryRe = new HashMap<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        List<String> joinKeyVals = new ArrayList<>();
                        for (CustomFieldV2 k : sampleJoinKeys) {
                            String val = rs.getString(k.getFieldName());
                            joinKeyVals.add(val);
                        }

                        if (queryRe.get(joinKeyVals) == null) {
                            List<CustomFieldV2> cfs = new ArrayList<>();
                            queryRe.put(joinKeyVals, cfs);

                            for (CustomFieldV2 addCol : dAddCols) {
                                String val = rs.getString(addCol.getFieldName());
                                CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), val, addCol.getSqlType(), addCol.isKey(), addCol.isNull(), true);
                                cfs.add(cf);
                            }
                        }
                    }
                }

                for (CustomRecordV2WithJoinKeys crj : recordV2WithJoinKeys) {
                    List<CustomFieldV2> addColVals = queryRe.getOrDefault(crj.getJoinKeyVals(), dAddCols);
                    crj.addJoinKeyToRecord(addColVals);
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }

        re.add(data);
    }

    @Override
    public void stop() {
        // do nothing
    }

    public static class CustomRecordV2WithJoinKeys {

        private final CustomRecordV2 recordV2;

        private final List<String>   joinKeyVals;

        public CustomRecordV2WithJoinKeys(CustomRecordV2 recordV2, List<String> joinKeyVals){
            this.recordV2 = recordV2;
            this.joinKeyVals = joinKeyVals;
        }

        public void addJoinKeyToRecord(List<CustomFieldV2> addCols) {
            if (recordV2.getBeforeColumns() != null && !recordV2.getBeforeColumns().isEmpty()) {
                recordV2.getBeforeColumns().addAll(addCols);
            }

            if (recordV2.getAfterColumns() != null && !recordV2.getAfterColumns().isEmpty()) {
                recordV2.getAfterColumns().addAll(addCols);
            }
        }

        public List<String> getJoinKeyVals() { return joinKeyVals; }
    }
}
