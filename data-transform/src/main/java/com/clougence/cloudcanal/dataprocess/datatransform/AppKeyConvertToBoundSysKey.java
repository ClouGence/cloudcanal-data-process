package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 纯粹打数据日志
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class AppKeyConvertToBoundSysKey implements CloudCanalProcessorV2 {
    protected static final Logger vehUserLogger = LoggerFactory.getLogger("AppKeyConvertToBoundSysKey");

    private SchemaInfo targetTable = new SchemaInfo(null, "uat_appserver", "APP_KEY");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        vehUserLogger.info("start process......");
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                if (recordV2.getAfterColumnMap().get("status").getValue() != null) {
                    String status = recordV2.getAfterColumnMap().get("status").getValue().toString();
                    if ("DISABLED".equals(status)) {
                        status = "0";
                    } else if ("ACTIVE".equals(status)) {
                        status = "1";
                    }
                    recordV2.getAfterColumnMap().get("status").setValue(status);
//                    newRecords.add(recordV2);
                }
            }
        }

        re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
