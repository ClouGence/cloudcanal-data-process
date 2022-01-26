package com.clougence.cloudcanal.dataprocess.datatransform;

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
 * 纯粹打数据日志
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class AppKeyConvertToBoundSysKey implements CloudCanalProcessorV2 {

    protected static final Logger vehUserLogger = LoggerFactory.getLogger("AppKeyConvertToBoundSysKey");

    private SchemaInfo            targetTable   = new SchemaInfo(null, "uat_appserver", "APP_KEY");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        vehUserLogger.info("start process......");
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            switch (data.getEventType()) {
                case INSERT: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getAfterColumnMap());
                    }
                    break;
                }
                case UPDATE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getBeforeColumnMap());
                    }

                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getAfterColumnMap());
                    }
                    break;
                }
                case DELETE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        changeStatusColumnValue(recordV2.getBeforeColumnMap());
                    }
                    break;
                }
                default:
                    break;
            }
        }

        re.add(data);
        return re;
    }

    protected void changeStatusColumnValue(LinkedHashMap<String, CustomFieldV2> columns) {
        Object aStatus = columns.get("status").getValue();
        if (aStatus != null) {
            String status = changeStatus(aStatus);
            columns.get("status").setValue(status);
        }
    }

    protected String changeStatus(Object oStatus) {
        String status = oStatus.toString();
        if ("DISABLED".equals(status)) {
            return "0";
        } else if ("ACTIVE".equals(status)) {
            return "1";
        } else {
            return status;
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
