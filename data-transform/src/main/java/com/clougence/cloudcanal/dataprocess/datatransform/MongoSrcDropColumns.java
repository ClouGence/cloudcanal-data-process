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

/**
 * 纯粹打数据日志
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class MongoSrcDropColumns implements CloudCanalProcessorV2 {

    private SchemaInfo targetTable = new SchemaInfo(null, "dingtax", "worker_stats");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                recordV2.dropField("cpu_stat");
                recordV2.dropField("mem_stat");
                recordV2.dropField("disk_stat");
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
