package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * 纯粹打数据日志
 *
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlSrcRenameColumns implements CloudCanalProcessorV2 {

    private SchemaInfo targetTable = new SchemaInfo(null, "dingtax", "worker_stats");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                changeColumnName(recordV2, "cpu_stat", "cpu_stat_rename");
                changeColumnName(recordV2, "mem_stat", "mem_stat_rename");
                changeColumnName(recordV2, "disk_stat", "disk_stat_rename");
            }
        }

        re.add(data);
        return re;
    }

    protected void changeColumnName(CustomRecordV2 recordV2, String srcName, String dstName) {
        CustomFieldV2 cpuAf = recordV2.getAfterColumnMap().get(srcName);
        if (cpuAf != null) {
            cpuAf.setFieldName(dstName);
        }

        CustomFieldV2 cpuBf = recordV2.getBeforeColumnMap().get(srcName);
        if (cpuBf != null) {
            cpuBf.setFieldName(dstName);
        }

        CustomFieldV2 cpuAkf = recordV2.getAfterKeyColumnMap().get(srcName);
        if (cpuAkf != null) {
            cpuAkf.setFieldName(dstName);
        }

        CustomFieldV2 cpuBkf = recordV2.getBeforeKeyColumnMap().get(srcName);
        if (cpuBkf != null) {
            cpuBkf.setFieldName(dstName);
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
