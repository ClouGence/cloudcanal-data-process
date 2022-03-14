package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Saint Kay
 * @date 2022/2/16
 */
public class MySqlSrcIgnoreVirtualColumn implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    private Map<SchemaInfo, List<String>> virtualColumnMap = new HashMap<>();


    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }
        DataSource dataSource = (DataSource) context.getSrcRdbDs();
        String sql = "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM information_schema.`COLUMNS` WHERE EXTRA = 'VIRTUAL GENERATED';";
        try {
            Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                SchemaInfo schemaInfo = new SchemaInfo(null, tableSchema, tableName);
                if (!virtualColumnMap.containsKey(schemaInfo)) {
                    virtualColumnMap.put(schemaInfo, new ArrayList<>());
                }
                virtualColumnMap.get(schemaInfo).add(columnName);
            }
        } catch (SQLException e) {
            customLogger.error(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }


    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (virtualColumnMap.containsKey(data.getSchemaInfo())) {
            List<String> virtualColumns = virtualColumnMap.get(data.getSchemaInfo());
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                for (String v : virtualColumns) {
                    recordV2.dropField(v);
                }
            }
        }
        re.add(data);
        return re;
    }

    @Override
    public void stop() {

    }


}
