package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.*;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class WideTableProcessorV2_simple implements CloudCanalProcessorV2 {

    private DataSource          srcDataSource;

    private SchemaInfo          factTable             = new SchemaInfo(null, "wide_table", "task_schedule");

    private SchemaInfo          dimensionTable        = new SchemaInfo(null, "wide_table", "worker");

    private CustomFieldV2       factTableJoinKey      = CustomFieldV2.buildField("worker_id", null, Types.BIGINT, false, false, true);

    private CustomFieldV2       dimensionTableJoinKey = CustomFieldV2.buildField("id", null, Types.BIGINT, true, false, true);;

    private List<CustomFieldV2> addCols               = new ArrayList<>();

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();

        addCols.add(CustomFieldV2.buildField("worker_state", null, Types.VARCHAR, false, true, true));
        addCols.add(CustomFieldV2.buildField("worker_name", null, Types.VARCHAR, false, true, true));
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
        if (data.getSchemaInfo().equals(factTable)) {
            handleFactTable(re, data);
        } else if (data.getSchemaInfo().equals(dimensionTable)) {
            handleDimensionTable(re, data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected void handleDimensionTable(List<CustomData> re, CustomData data) {
        re.add(data);

        if (data.getEventType() == EventTypeInSdk.UPDATE) {
            List<CustomRecordV2> records = new ArrayList<>();

            for (CustomRecordV2 recordV2 : data.getRecords()) {
                List<CustomFieldV2> factJoinKeys = new ArrayList<>();
                List<CustomFieldV2> factAddCols = new ArrayList<>();

                CustomFieldV2 jf = findCol(recordV2.getBeforeKeyColumns(), dimensionTableJoinKey.getFieldName());
                CustomFieldV2 factJoinKey = CustomFieldV2
                    .buildField(factTableJoinKey.getFieldName(), jf.getValue(), factTableJoinKey.getSqlType(), true, jf.getValue() == null, false);

                factAddCols.add(factJoinKey);
                factJoinKeys.add(factJoinKey);
                for (CustomFieldV2 addCol : addCols) {
                    CustomFieldV2 af = findCol(recordV2.getAfterColumns(), addCol.getFieldName());
                    factAddCols.add(CustomFieldV2.buildField(addCol.getFieldName(), af.getValue(), addCol.getSqlType(), addCol.isKey(), af.getValue() == null, true));
                }

                CustomRecordV2 r = CustomRecordV2.buildRecord(factAddCols, factAddCols, factJoinKeys, factJoinKeys);
                records.add(r);
            }

            CustomData newD = new CustomData(factTable, EventTypeInSdk.UPDATE, records);
            re.add(newD);
        } else if (data.getEventType() == EventTypeInSdk.DELETE) {
            List<CustomRecordV2> records = new ArrayList<>();

            for (CustomRecordV2 recordV2 : data.getRecords()) {
                List<CustomFieldV2> factJoinKeys = new ArrayList<>();
                List<CustomFieldV2> factAddCols = new ArrayList<>();

                CustomFieldV2 jf = findCol(recordV2.getBeforeKeyColumns(), dimensionTableJoinKey.getFieldName());
                CustomFieldV2 factJoinKey = CustomFieldV2
                    .buildField(factTableJoinKey.getFieldName(), jf.getValue(), factTableJoinKey.getSqlType(), true, jf.getValue() == null, false);

                factAddCols.add(factJoinKey);
                factJoinKeys.add(factJoinKey);
                for (CustomFieldV2 addCol : addCols) {
                    factAddCols.add(CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true));
                }

                CustomRecordV2 r = CustomRecordV2.buildRecord(factAddCols, factAddCols, factJoinKeys, factJoinKeys);
                records.add(r);
            }

            CustomData newD = new CustomData(factTable, EventTypeInSdk.UPDATE, records);
            re.add(newD);
        }

    }

    protected String genDimensionQuerySql() {
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;

        for (CustomFieldV2 addCol : addCols) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append("`").append(addCol.getFieldName()).append("`");
        }

        sb.append(" FROM ");
        sb.append("`").append(dimensionTable.getSchema()).append("`");
        sb.append(".").append("`").append(dimensionTable.getTable()).append("`");
        sb.append(" WHERE ");
        sb.append("`").append(dimensionTableJoinKey.getFieldName()).append("`=?");
        return sb.toString();
    }

    protected void handleFactTable(List<CustomData> re, CustomData data) {
        String sql = genDimensionQuerySql();
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = findCol(recordV2.getAfterColumns(), factTableJoinKey.getFieldName());
                    break;
                case DELETE:
                    f = findCol(recordV2.getBeforeColumns(), factTableJoinKey.getFieldName());
                    break;
                default:
                    throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
            }

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setObject(1, f.getValue(), dimensionTableJoinKey.getSqlType());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        for (CustomFieldV2 addCol : addCols) {
                            String val = rs.getString(addCol.getFieldName());
                            CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), val, addCol.getSqlType(), addCol.isKey(), val == null, true);
                            addJoinKeyToRecord(recordV2, cf);
                        }
                    } else {
                        // add empty cols
                        for (CustomFieldV2 addCol : addCols) {
                            CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true);
                            addJoinKeyToRecord(recordV2, cf);
                        }
                    }
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

    public void addJoinKeyToRecord(CustomRecordV2 recordV2, CustomFieldV2 addCol) {
        if (recordV2.getBeforeColumns() != null && !recordV2.getBeforeColumns().isEmpty()) {
            recordV2.getBeforeColumns().add(addCol);
        }

        if (recordV2.getAfterColumns() != null && !recordV2.getAfterColumns().isEmpty()) {
            recordV2.getAfterColumns().add(addCol);
        }
    }
}
