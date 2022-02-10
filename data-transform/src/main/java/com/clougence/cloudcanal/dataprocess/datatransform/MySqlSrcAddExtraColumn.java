package com.clougence.cloudcanal.dataprocess.datatransform;

import java.sql.Types;
import java.util.ArrayList;
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
public class MySqlSrcAddExtraColumn implements CloudCanalProcessorV2 {

    private SchemaInfo targetTable = new SchemaInfo(null, "dingtax", "worker_stats");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                recordV2.addField(CustomFieldV2.buildField("extra_col_bigint", 123, Types.BIGINT, false, false, true));
                recordV2.addField(CustomFieldV2.buildField("extra_col_varchar", "added_str", Types.VARCHAR, false, false, true));
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
