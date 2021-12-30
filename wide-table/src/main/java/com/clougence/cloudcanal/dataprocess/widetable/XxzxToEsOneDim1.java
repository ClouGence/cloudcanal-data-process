package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.*;

/**
 * @author bucketli 2021/12/29 18:47:47
 */
public class XxzxToEsOneDim1 implements CloudCanalProcessorV2 {

    protected static final Logger log                     = LoggerFactory.getLogger("custom_processor");

    private DataSource            srcDataSource;

    // 事实表定义(fact table definition)

    private SchemaInfo            factTable               = new SchemaInfo(null, "dingtax", "xxzx_xxzb");

    // 第二个维表定义(2nd dimension table definition)

    private SchemaInfo            dimensionTable_2        = new SchemaInfo(null, "dingtax", "bi_dm_swjg");

    // 第二个维表关联字段在事实表上的定义(2nd dimension table's join key definition in fact table)

    private CustomFieldV2         factTableJoinKey_2      = CustomFieldV2.buildField("xnzz_id", null, Types.BIGINT, false, false, true);

    // 第二个维表关联字段在维表上的字段定义(2nd dimension table's join key definition in dimension table)

    private CustomFieldV2         dimensionTableJoinKey_2 = CustomFieldV2.buildField("xnzz_id", null, Types.BIGINT, false, false, true);;

    private List<CustomFieldV2>   srcCols_2               = new ArrayList<>();

    private List<CustomFieldV2>   addCols_2               = new ArrayList<>();

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();

        srcCols_2.add(CustomFieldV2.buildField("swjg_dm_path", null, Types.VARCHAR, false, true, true));
        addCols_2.add(CustomFieldV2.buildField("swjg_dms", null, Types.VARCHAR, false, true, true));
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(factTable)) {
            data = handleFactTable(data, dimensionTable_2, factTableJoinKey_2, dimensionTableJoinKey_2, addCols_2, srcCols_2);
            re.add(data);
        } else {
            re.add(data);
        }

        return re;

    }

    protected String genDimensionQuerySql(List<CustomFieldV2> srcCols, SchemaInfo dimensionTable, CustomFieldV2 dimensionTableJoinKey) {
        StringBuilder sb = new StringBuilder("SELECT `swjg_dm`");
        for (CustomFieldV2 addCol : srcCols) {
            sb.append(",");
            sb.append("`").append(addCol.getFieldName()).append("`");
        }

        sb.append(" FROM ");
        sb.append("`").append(dimensionTable.getSchema()).append("`");
        sb.append(".").append("`").append(dimensionTable.getTable()).append("`");
        sb.append(" WHERE ");
        sb.append("`").append(dimensionTableJoinKey.getFieldName()).append("`=?");
        sb.append(" and sybz=1 and qybz=1 and swjg_dm not like '199%'");
        return sb.toString();
    }

    protected CustomData handleFactTable(CustomData data, SchemaInfo dimensionTable, CustomFieldV2 factTableJoinKey, CustomFieldV2 dimensionTableJoinKey,
                                         List<CustomFieldV2> addCols, List<CustomFieldV2> srcCols) {
        String sql = genDimensionQuerySql(srcCols, dimensionTable, dimensionTableJoinKey);
        List<CustomRecordV2> newRecords = new ArrayList<>();
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 f = fetchNewestField(data.getEventType(), recordV2, factTableJoinKey.getFieldName());

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setObject(1, f.getValue(), dimensionTableJoinKey.getSqlType());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        CustomRecordV2 nOne = genNewRecord(recordV2, f, data.getEventType(), rs, addCols, srcCols);
                        newRecords.add(nOne);

                        while (rs.next()) {
                            CustomRecordV2 more = genNewRecord(recordV2, f, data.getEventType(), rs, addCols, srcCols);
                            newRecords.add(more);
                        }
                    } else {
                        for (CustomFieldV2 addCol : addCols) {
                            // 如果没查到记录改主键值么？
                            CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true);
                            recordV2.addField(cf);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }

        }

        return new CustomData(data.getSchemaInfo(), data.getEventType(), newRecords);
    }

    protected CustomRecordV2 genNewRecord(CustomRecordV2 origin, CustomFieldV2 joinKey, EventTypeInSdk eventType, ResultSet rs, List<CustomFieldV2> addCols,
                                          List<CustomFieldV2> srcCols) throws SQLException {
        CustomRecordV2 newOne = deepCopyRecord(origin);
        CustomFieldV2 pk = fetchNewestField(eventType, newOne, "id");

        for (int i = 0; i < addCols.size(); i++) {
            CustomFieldV2 srcCol = srcCols.get(i);
            String swjgDmVal = rs.getString("swjg_dm");
            String val = rs.getString(srcCol.getFieldName());
            if (val != null) {
                String[] swjgDms = val.split("-");
                String s = "[" + StringUtils.join(swjgDms, ",") + "]";

                CustomFieldV2 addCol = addCols.get(i);
                CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), s, addCol.getSqlType(), addCol.isKey(), false, true);
                newOne.addField(cf);

                String newPk = pk.getValue().toString().concat(joinKey.getValue().toString()).concat(swjgDmVal);

                // 使用你自己的主键生成策略
                long newPkNum = Math.abs(newPk.hashCode());

                fillColVal(newOne, "id", newPkNum);
            } else {
                // 如果查到的记录里面值为空改主键么？
                CustomFieldV2 addCol = addCols.get(i);
                CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true);
                newOne.addField(cf);
            }
        }

        return newOne;
    }

    protected void fillColVal(CustomRecordV2 target, String col, Object newVal) {
        CustomFieldV2 ak = target.getAfterKeyColumnMap().get(col);
        if (ak != null) {
            ak.setValue(newVal);
        }

        CustomFieldV2 ac = target.getAfterColumnMap().get(col);
        if (ac != null) {
            ac.setValue(newVal);
        }

        CustomFieldV2 bk = target.getBeforeKeyColumnMap().get(col);
        if (bk != null) {
            bk.setValue(newVal);
        }

        CustomFieldV2 bc = target.getBeforeColumnMap().get(col);
        if (bc != null) {
            bc.setValue(newVal);
        }
    }

    protected CustomRecordV2 deepCopyRecord(CustomRecordV2 origin) {
        CustomRecordV2 newOne = new CustomRecordV2();
        newOne.setBeforeColumnMap(deepCopyFields(origin.getBeforeColumnMap()));
        newOne.setBeforeKeyColumnMap(deepCopyFields(origin.getBeforeKeyColumnMap()));
        newOne.setAfterColumnMap(deepCopyFields(origin.getAfterColumnMap()));
        newOne.setAfterKeyColumnMap(deepCopyFields(origin.getAfterKeyColumnMap()));
        return newOne;
    }

    protected LinkedHashMap<String, CustomFieldV2> deepCopyFields(LinkedHashMap<String, CustomFieldV2> fields) {
        LinkedHashMap<String, CustomFieldV2> fs = new LinkedHashMap<>();
        for (CustomFieldV2 f : fields.values()) {
            CustomFieldV2 newF = CustomFieldV2.buildField(f.getFieldName(), f.getValue(), f.getSqlType(), f.isKey(), f.isNull(), f.isUpdated());
            fs.put(newF.getFieldName(), newF);
        }

        return fs;
    }

    protected CustomFieldV2 fetchNewestField(EventTypeInSdk eventType, CustomRecordV2 recordV2, String fieldName) {
        switch (eventType) {
            case INSERT:
            case UPDATE:
                return recordV2.getAfterColumnMap().get(fieldName);
            case DELETE:
                return recordV2.getBeforeColumnMap().get(fieldName);
            default:
                throw new IllegalArgumentException("unsupported event type:" + eventType);
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
