package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessor;
import com.clougence.cloudcanal.sdk.api.constant.rdb.RecordAction;
import com.clougence.cloudcanal.sdk.api.contextkey.RdbContextKey;
import com.clougence.cloudcanal.sdk.api.metakey.RdbMetaKey;
import com.clougence.cloudcanal.sdk.api.model.CustomField;
import com.clougence.cloudcanal.sdk.api.model.CustomProcessorContext;
import com.clougence.cloudcanal.sdk.api.model.CustomRecord;
import com.clougence.cloudcanal.sdk.api.service.impl.RecordBuilder;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class OneFactTableOneDimensionTableProcessor implements CloudCanalProcessor {

    @Override
    public List<CustomRecord> process(List<CustomRecord> records, CustomProcessorContext context) {
        DataSource ds = (DataSource) context.getProcessorContextMap().get(RdbContextKey.SOURCE_DATASOURCE);
        try (Connection conn = ds.getConnection()) {
            for (CustomRecord record : records) {
                String schema = (String) record.getRecordMetaMap().get(RdbMetaKey.SCHEMA_NAME);
                String table = (String) record.getRecordMetaMap().get(RdbMetaKey.TABLE_NAME);
                RecordAction action = (RecordAction) record.getRecordMetaMap().get(RdbMetaKey.ACTION_NAME);

                if (schema.equals("wide_table") && table.equals("task_schedule")) {
                    CustomField pk;
                    switch (action) {
                        case INSERT:
                        case UPDATE:
                            pk = record.getFieldMapAfter().get("worker_id");
                            break;
                        case DELETE:
                            pk = record.getFieldMapBefore().get("worker_id");
                            break;
                        default:
                            continue;
                    }

                    try (PreparedStatement ps = conn.prepareStatement("select * from wide_table.worker where id=?")) {
                        ps.setString(1, (String) pk.getValue());
                        ResultSet rs = ps.executeQuery();
                        while (rs.next()) {
                            Map<String, Object> addFieldValueMap = new LinkedHashMap<>();
                            addFieldValueMap.put("worker_state", rs.getString("worker_state"));
                            addFieldValueMap.put("worker_name", rs.getString("worker_name"));
                            RecordBuilder.modifyRecordBuilder(record).addField(addFieldValueMap);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        return records;
    }
}
