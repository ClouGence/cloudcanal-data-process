package com.clougence.cloudcanal.dataprocess.datatransform;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * @author bucketli 2022/2/23 17:59:25
 */
public class FillAndChangeTableValue implements CloudCanalProcessorV2 {

    protected static final Logger akSk     = LoggerFactory.getLogger("CUSTOM_PROCESS");

    private SchemaInfo            srcTable = new SchemaInfo(null, "uat_appserver", "APP_KEY");

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        List<CustomRecordV2> newRecords = new ArrayList<>();
        if (data.getSchemaInfo().equals(srcTable)) {
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getAfterColumnMap());
                        newRecords.add(recordV2);
                    }
                    break;
                }
                case DELETE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getBeforeColumnMap());
                        newRecords.add(recordV2);
                    }
                    break;
                }
                default:
                    break;
            }
        }
        CustomData newData = new CustomData(srcTable, data.getEventType(), newRecords);
        re.add(newData);
        return re;
    }

    protected void changeStatusColumnValue(LinkedHashMap<String, CustomFieldV2> columns) {
        fillNullCreateTime(columns);
        fillNullCreatedBy(columns);
        changeActiveStatus(columns);
    }

    private void fillNullCreateTime(LinkedHashMap<String, CustomFieldV2> columns) {
        if (columns.get("createdTime").getValue() == null) {
            columns.get("createdTime").setValue(fetchStrNowTime());
            columns.get("createdTime").setNull(false);
            columns.get("createdTime").setKey(false);
            columns.get("createdTime").setSqlType(Types.TIMESTAMP);
            columns.get("createdTime").setUpdated(true);
        }
    }

    private void fillNullCreatedBy(LinkedHashMap<String, CustomFieldV2> columns) {
        if (columns.get("createdBy").getValue() == null) {
            columns.get("createdBy").setValue("system");
            columns.get("createdBy").setNull(false);
            columns.get("createdBy").setKey(false);
            columns.get("createdBy").setSqlType(Types.VARCHAR);
            columns.get("createdBy").setUpdated(true);
        }
    }

    private void changeActiveStatus(LinkedHashMap<String, CustomFieldV2> columns) {
        Object aStatus = columns.get("status").getValue();
        if (aStatus != null) {
            String val = String.valueOf(aStatus);
            if ("ACTIVE".equals(val)) {
                columns.get("status").setValue("1");
                columns.get("status").setNull(false);
                columns.get("status").setKey(false);
                columns.get("status").setSqlType(Types.VARCHAR);
                columns.get("status").setUpdated(true);
            }
        }
    }

    private String fetchStrNowTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.now().format(formatter);
    }

    @Override
    public void stop() {

    }
}
