package com.clougence.cloudcanal.dataprocess.widetable;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bucketli 2022/2/25 13:59:02
 */
public class MySqlToCh_user_1 implements CloudCanalProcessorV2 {

    private DataSource          srcDataSource;

    private final SchemaInfo    factTable = new SchemaInfo(null, "trade", "rhr_person_info");

    private final CustomFieldV2 distCol   = CustomFieldV2.buildField("area_name", null, Types.VARCHAR, false, true, true);
    private final CustomFieldV2 organCol  = CustomFieldV2.buildField("organ_name", null, Types.VARCHAR, false, true, true);

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(factTable)) {
            addExtraInfo(data);
            re.add(data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected void addExtraInfo(CustomData data) {
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 areaIdCol;
            CustomFieldV2 organIdCol;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE: {
                    areaIdCol = recordV2.getAfterColumnMap().get("area_id");
                    organIdCol = recordV2.getAfterColumnMap().get("organ_id");
                    break;
                }
                case DELETE:
                default:
                    continue;
            }

            addDistCol(areaIdCol, recordV2);
            addOrganCol(organIdCol, recordV2);
        }
    }

    String districtSql = "select `name` from `trade`.`admin_district_info` where `id`=?";
    String organSql    = "select `name` from `trade`.`admin_organ` where `id`=?";

    protected void addDistCol(CustomFieldV2 areaIdCol, CustomRecordV2 recordV2) {
        try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(districtSql)) {
            ps.setObject(1, areaIdCol.getValue(), Types.BIGINT);
            try (ResultSet rs = ps.executeQuery()) {
                String val = null;
                if (rs.next()) {
                    val = rs.getString("name");
                }

                CustomFieldV2 cf = CustomFieldV2.buildField(distCol.getFieldName(), val, distCol.getSqlType(), distCol.isKey(), val == null, true);
                recordV2.addField(cf);
            }
        } catch (Exception e) {
            throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    protected void addOrganCol(CustomFieldV2 organIdCol, CustomRecordV2 recordV2) {
        try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(organSql)) {
            ps.setObject(1, organIdCol.getValue(), Types.BIGINT);
            try (ResultSet rs = ps.executeQuery()) {
                String val = null;
                if (rs.next()) {
                    val = rs.getString("name");
                }

                CustomFieldV2 cf = CustomFieldV2.buildField(organCol.getFieldName(), val, organCol.getSqlType(), organCol.isKey(), val == null, true);
                recordV2.addField(cf);
            }
        } catch (Exception e) {
            throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
