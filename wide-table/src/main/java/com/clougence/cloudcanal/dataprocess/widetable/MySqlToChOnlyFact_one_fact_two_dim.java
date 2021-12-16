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

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToChOnlyFact_one_fact_two_dim implements CloudCanalProcessorV2 {

    private DataSource          srcDataSource;

    // 事实表定义(fact table definition)
    private SchemaInfo          factTable               = new SchemaInfo(null, "trade", "my_order");

    // 第一个维表定义(1st dimension table definition)
    private SchemaInfo          dimensionTable_1        = new SchemaInfo(null, "trade", "user");

    // 第二个维表定义(2nd dimension table definition)
    private SchemaInfo          dimensionTable_2        = new SchemaInfo(null, "trade", "product");

    // 第一个维表关联字段在事实表上的定义(1st dimension table's join key definition in fact table)
    private CustomFieldV2       factTableJoinKey_1      = CustomFieldV2.buildField("user_id", null, Types.BIGINT, false, false, true);

    // 第一个维表关联字段在维表上的字段定义(1st dimension table's join key definition in dimension table)
    private CustomFieldV2       dimensionTableJoinKey_1 = CustomFieldV2.buildField("id", null, Types.BIGINT, true, false, true);;

    // 第二个维表关联字段在事实表上的定义(2nd dimension table's join key definition in fact table)
    private CustomFieldV2       factTableJoinKey_2      = CustomFieldV2.buildField("product_id", null, Types.BIGINT, false, false, true);

    // 第二个维表关联字段在维表上的字段定义(2nd dimension table's join key definition in dimension table)
    private CustomFieldV2       dimensionTableJoinKey_2 = CustomFieldV2.buildField("id", null, Types.BIGINT, true, false, true);;

    private List<CustomFieldV2> addCols_1               = new ArrayList<>();

    private List<CustomFieldV2> srcCols_1               = new ArrayList<>();

    private List<CustomFieldV2> srcCols_2               = new ArrayList<>();

    private List<CustomFieldV2> addCols_2               = new ArrayList<>();

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();

        // 第一个维表需要添加的字段在维表上的名字(columns of 1st dimension table add to fact table ,those definition on dimension table)
        srcCols_1.add(CustomFieldV2.buildField("name", null, Types.VARCHAR, false, true, true));
        // 第一个维表需要添加的字段在事实表上的名字(columns of 1st dimension table add to fact table ,those definition on fact table)
        addCols_1.add(CustomFieldV2.buildField("user_name", null, Types.VARCHAR, false, true, true));

        // 第二个维表需要添加的字段在维表上的名字(columns of 2nd dimension table add to fact table ,those definition on dimension table)
        srcCols_2.add(CustomFieldV2.buildField("name", null, Types.VARCHAR, false, true, true));
        srcCols_2.add(CustomFieldV2.buildField("price", null, Types.DECIMAL, false, true, true));
        // 第二个维表需要添加的字段在事实表上的名字(columns of 2nd dimension table add to fact table ,those definition on fact table)
        addCols_2.add(CustomFieldV2.buildField("product_name", null, Types.VARCHAR, false, true, true));
        addCols_2.add(CustomFieldV2.buildField("product_price", null, Types.DECIMAL, false, true, true));
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(factTable)) {
            handleFactTable(data, dimensionTable_1, factTableJoinKey_1, dimensionTableJoinKey_1, addCols_1, srcCols_1);
            handleFactTable(data, dimensionTable_2, factTableJoinKey_2, dimensionTableJoinKey_2, addCols_2, srcCols_2);
            re.add(data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected String genDimensionQuerySql(List<CustomFieldV2> srcCols, SchemaInfo dimensionTable, CustomFieldV2 dimensionTableJoinKey) {
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
        return sb.toString();
    }

    protected void handleFactTable(CustomData data, SchemaInfo dimensionTable, CustomFieldV2 factTableJoinKey, CustomFieldV2 dimensionTableJoinKey,
                                   List<CustomFieldV2> addCols, List<CustomFieldV2> srcCols) {
        String sql = genDimensionQuerySql(srcCols, dimensionTable, dimensionTableJoinKey);
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

                            CustomFieldV2 addCol = addCols.get(i);
                            CustomFieldV2 cf = CustomFieldV2.buildField(addCol.getFieldName(), val, addCol.getSqlType(), addCol.isKey(), val == null, true);
                            recordV2.addField(cf);
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
