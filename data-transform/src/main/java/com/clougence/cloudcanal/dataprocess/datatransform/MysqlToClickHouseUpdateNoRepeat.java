package com.clougence.cloudcanal.dataprocess.datatransform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessor;
import com.clougence.cloudcanal.sdk.api.constant.rdb.RecordAction;
import com.clougence.cloudcanal.sdk.api.contextkey.RdbContextKey;
import com.clougence.cloudcanal.sdk.api.metakey.RdbMetaKey;
import com.clougence.cloudcanal.sdk.api.model.CustomField;
import com.clougence.cloudcanal.sdk.api.model.CustomProcessorContext;
import com.clougence.cloudcanal.sdk.api.model.CustomRecord;

/**
 * 功能描述:mysql同步到clickHouse防止修改重复数据
 *
 * @author yisai
 * @Date 2021-12-23 17:23
 */
public class MysqlToClickHouseUpdateNoRepeat implements CloudCanalProcessor {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");
    /**
     * 库
     */
    private String                schemaName;
    /**
     * 表
     */
    private String                tableName;
    /**
     * 主键
     */
    private Long                  primaryKey;
    /**
     * 动作
     */
    private String                actionName;

    @Override
    public List<CustomRecord> process(List<CustomRecord> customRecordList, CustomProcessorContext customProcessorContext) {
        // 修改同步前先删除对端库老数据
        updateRecord(customRecordList, (DataSource) customProcessorContext.getProcessorContextMap().get(RdbContextKey.TARGET_DATASOURCE));
        return customRecordList;
    }

    /**
     * 功能描述:修改同步前先删除对端库老数据 注意主键要放在表第一个字段
     *
     * @author yisai
     * @Date 2021-12-23 17:00
     */
    private void updateRecord(List<CustomRecord> customRecordList, DataSource dataSource) {
        try {
            Connection connection = dataSource.getConnection();
            for (CustomRecord customRecord : customRecordList) {
                actionName = customRecord.getRecordMetaMap().get(RdbMetaKey.ACTION_NAME).toString();
                if (RecordAction.UPDATE.name().equals(actionName)) {
                    schemaName = customRecord.getRecordMetaMap().get(RdbMetaKey.SCHEMA_NAME).toString();
                    tableName = customRecord.getRecordMetaMap().get(RdbMetaKey.TABLE_NAME).toString();
                    int endIndex = customRecord.getFieldMapBefore().toString().indexOf("=");
                    String primaryKeyName = customRecord.getFieldMapBefore().toString().substring(1, endIndex);
                    CustomField primaryKeyCustomField = customRecord.getFieldMapBefore().get(primaryKeyName);
                    primaryKey = Long.parseLong(primaryKeyCustomField.getValue().toString());
                    PreparedStatement ps = connection.prepareStatement("ALTER TABLE " + schemaName + "." + tableName + " DELETE WHERE " + primaryKeyName + " = " + primaryKey);
                    ps.execute();
                }
            }
        } catch (Exception e) {
            customLogger.error(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }
}
