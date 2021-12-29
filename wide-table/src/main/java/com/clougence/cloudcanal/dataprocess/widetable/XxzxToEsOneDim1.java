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
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author bucketli 2021/12/29 18:47:47
 */
public class XxzxToEsOneDim1 implements CloudCanalProcessorV2 {

    private DataSource          srcDataSource;

    // 事实表定义(fact table definition)

    private SchemaInfo          factTable               = new SchemaInfo(null, "dingtax", "xxzx_xxzb");

    // 第二个维表定义(2nd dimension table definition)

    private SchemaInfo          dimensionTable_2        = new SchemaInfo(null, "dingtax", "bi_dm_swjg");

    // 第二个维表关联字段在事实表上的定义(2nd dimension table's join key definition in fact table)

    private CustomFieldV2       factTableJoinKey_2      = CustomFieldV2.buildField("xnzz_id", null, Types.BIGINT, false, false, true);

    // 第二个维表关联字段在维表上的字段定义(2nd dimension table's join key definition in dimension table)

    private CustomFieldV2       dimensionTableJoinKey_2 = CustomFieldV2.buildField("xnzz_id", null, Types.BIGINT, false, false, true);;

    private List<CustomFieldV2> srcCols_2               = new ArrayList<>();

    private List<CustomFieldV2> addCols_2               = new ArrayList<>();

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
            handleFactTable(2, data, dimensionTable_2, factTableJoinKey_2, dimensionTableJoinKey_2, addCols_2, srcCols_2);
            re.add(data);
        } else {
            re.add(data);
        }

        return re;

    }

    /**
     * @param type 1 关联部门，2关联机关
     */
    protected String genDimensionQuerySql(int type, List<CustomFieldV2> srcCols, SchemaInfo dimensionTable, CustomFieldV2 dimensionTableJoinKey) {
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (CustomFieldV2 addCol : srcCols) {
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
        sb.append(" and sybz=1 and qybz=1 and swjg_dm not like '199%'");
        return sb.toString();
    }

    protected void handleFactTable(int type, CustomData data, SchemaInfo dimensionTable, CustomFieldV2 factTableJoinKey, CustomFieldV2 dimensionTableJoinKey,
                                   List<CustomFieldV2> addCols, List<CustomFieldV2> srcCols) {
        String sql = genDimensionQuerySql(type, srcCols, dimensionTable, dimensionTableJoinKey);
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = recordV2.getAfterColumnMap().get(factTableJoinKey.getFieldName());
                    break;
                case DELETE:
                    f = recordV2.getBeforeColumnMap().get(factTableJoinKey.getFieldName());
                    break;
                default:
                    throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
            }

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setObject(1, f.getValue(), dimensionTableJoinKey.getSqlType());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        for (int i = 0; i < addCols.size(); i++) {
                            CustomFieldV2 srcCol = srcCols.get(i);
                            String val = rs.getString(srcCol.getFieldName());
                            if (val != null) {
                                String[] swjgDms = val.split("-");
                                List<Long> longDms = new ArrayList<>();
                                for (String d : swjgDms) {
                                    longDms.add(Long.valueOf(d));
                                }

                                CustomFieldV2 addCol = addCols.get(i);
                                CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(),longDms.toArray(), addCol.getSqlType(), addCol.isKey(), false, true);
                                recordV2.addField(cf);
                            } else {
                                CustomFieldV2 addCol = addCols.get(i);
                                CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true);
                                recordV2.addField(cf);
                            }
                        }
                    } else {
                        // add empty cols
                        for (CustomFieldV2 addCol : addCols) {
                            CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), null, addCol.getSqlType(), addCol.isKey(), true, true);
                            recordV2.addField(cf);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
