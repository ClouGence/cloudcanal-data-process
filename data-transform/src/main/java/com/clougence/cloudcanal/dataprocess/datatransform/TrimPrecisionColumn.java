package com.clougence.cloudcanal.dataprocess.datatransform;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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
public class TrimPrecisionColumn implements CloudCanalProcessorV2 {

    private SchemaInfo srcTable = new SchemaInfo(null, "uniq_test", "batch_step_execution");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        if (data.getSchemaInfo().equals(srcTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                switch (data.getEventType()) {
                    case INSERT: {
                        trimColumnsTime(recordV2.getAfterColumnMap());
                        break;
                    }
                    case UPDATE: {
                        trimColumnsTime(recordV2.getBeforeColumnMap());
                        trimColumnsTime(recordV2.getAfterColumnMap());
                        break;
                    }
                    default:
                        break;
                }
            }
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    protected void trimColumnsTime(LinkedHashMap<String, CustomFieldV2> columnMap) {
        CustomFieldV2 f1 = columnMap.get("START_TIME");
        if (f1 != null && f1.getValue() != null) {
            String val = trimTime((String) (f1.getValue()));
            f1.setValue(val);
        }

        CustomFieldV2 f2 = columnMap.get("END_TIME");
        if (f2 != null && f2.getValue() != null) {
            String val = trimTime((String) (f2.getValue()));
            f2.setValue(val);
        }

        CustomFieldV2 f3 = columnMap.get("LAST_UPDATED");
        if (f3 != null && f3.getValue() != null) {
            String val = trimTime((String) (f3.getValue()));
            f3.setValue(val);
        }
    }

    DateTimeFormatter prev      = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private String trimTime(String d) {
        return LocalDateTime.parse(d, prev).format(formatter);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
