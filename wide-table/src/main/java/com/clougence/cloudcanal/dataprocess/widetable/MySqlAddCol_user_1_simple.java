package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * @author bucketli 2022/3/14 15:03:35
 */
public class MySqlAddCol_user_1_simple implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    private DataSource            srcDataSource;

    private final SchemaInfo      factTable    = new SchemaInfo(null, "trade", "my_order");

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
            fillUserInfo(data);
            fillProductInfo(data);
            re.add(data);
        } else {
            re.add(data);
        }

        return re;
    }

    String userSql = "select name from trade.user where id=?";

    protected void fillUserInfo(CustomData data) {
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = recordV2.getAfterColumnMap().get("user_id");
                    break;
                case DELETE:
                    f = recordV2.getBeforeColumnMap().get("user_id");
                    break;
                default:
                    throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
            }

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(userSql)) {
                ps.setObject(1, f.getValue(), Types.BIGINT);
                try (ResultSet rs = ps.executeQuery()) {
                    String userNameVal = null;
                    if (rs.next()) {
                        userNameVal = rs.getString("name");
                    }

                    CustomFieldV2 cf = CustomFieldV2.buildField("user_name", userNameVal, Types.VARCHAR, false, userNameVal == null, true);
                    recordV2.addField(cf);
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }

    String productSql = "select name,price from trade.product where id=?";

    protected void fillProductInfo(CustomData data) {
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = recordV2.getAfterColumnMap().get("product_id");
                    break;
                case DELETE:
                    f = recordV2.getBeforeColumnMap().get("product_id");
                    break;
                default:
                    throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
            }

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(productSql)) {
                ps.setObject(1, f.getValue(), Types.BIGINT);
                try (ResultSet rs = ps.executeQuery()) {
                    String productNameVal = null;
                    String productPrice = null;
                    if (rs.next()) {
                        productNameVal = rs.getString("name");
                        productPrice = rs.getString("price");
                    }

                    CustomFieldV2 cf = CustomFieldV2.buildField("product_name", productNameVal, Types.VARCHAR, false, productNameVal == null, true);
                    recordV2.addField(cf);

                    CustomFieldV2 cf2 = CustomFieldV2.buildField("product_price", productPrice, Types.DECIMAL, false, productPrice == null, true);
                    recordV2.addField(cf2);
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
