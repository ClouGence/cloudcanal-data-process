package com.clougence.cloudcanal.dataprocess.datagather;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * 对端主键为源端主键+额外字段 region。从而达到多地汇聚数据不冲突的目的。
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlPartitionToMySql1_user_1 implements CloudCanalProcessorV2 {

    private SchemaInfo    targetTable = new SchemaInfo(null, "shard_1", "my_order");

    private CustomFieldV2 region      = CustomFieldV2.buildField("region", "shanghai", Types.VARCHAR, true, false, false);

    @Override
    public void start(ProcessorContext context) {
        // do nothing
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            handleFactTable(re, data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected void handleFactTable(List<CustomData> re, CustomData data) {
        Iterator<CustomRecordV2> it = data.getRecords().iterator();
        while (it.hasNext()) {
            CustomRecordV2 recordV2 = it.next();
            CustomFieldV2 dataRegion = CustomFieldV2.buildField(region.getFieldName(), region.getValue(), region.getSqlType(), region.isKey(), region.isNull(), region.isUpdated());
            switch (data.getEventType()) {
                case INSERT: {
                    recordV2.getAfterColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    recordV2.getAfterKeyColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    break;
                }
                case UPDATE: {
                    recordV2.getAfterColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    recordV2.getAfterKeyColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    recordV2.getBeforeColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    recordV2.getBeforeKeyColumnMap().put(dataRegion.getFieldName(), dataRegion);
                }
                case DELETE: {
                    recordV2.getBeforeColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    recordV2.getBeforeKeyColumnMap().put(dataRegion.getFieldName(), dataRegion);
                    break;
                }
                default: {
                    break;
                }
            }
        }

        re.add(data);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
