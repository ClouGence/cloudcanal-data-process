package com.clougence.cloudcanal.dataprocess.datatransform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.*;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToChNoUpdate implements CloudCanalProcessorV2 {

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getEventType() == EventTypeInSdk.UPDATE) {

            for (CustomRecordV2 oriRecord : data.getRecords()) {
                CustomRecordV2 deleteRecord = new CustomRecordV2();
                CustomRecordV2 insertRecord = new CustomRecordV2();
                for (Map.Entry<String, CustomFieldV2> f : oriRecord.getBeforeColumnMap().entrySet()) {
                    fillColumnMap(f.getValue(), deleteRecord.getBeforeColumnMap());
                }

                for (Map.Entry<String, CustomFieldV2> f : oriRecord.getBeforeKeyColumnMap().entrySet()) {
                    fillColumnMap(f.getValue(), deleteRecord.getBeforeKeyColumnMap());
                }

                for (Map.Entry<String, CustomFieldV2> f : oriRecord.getAfterColumnMap().entrySet()) {
                    fillColumnMap(f.getValue(), insertRecord.getAfterColumnMap());
                }

                for (Map.Entry<String, CustomFieldV2> f : oriRecord.getAfterKeyColumnMap().entrySet()) {
                    fillColumnMap(f.getValue(), insertRecord.getAfterKeyColumnMap());
                }

                List<CustomRecordV2> deleteRecords = new ArrayList<>();
                List<CustomRecordV2> insertRecords = new ArrayList<>();
                deleteRecords.add(deleteRecord);
                insertRecords.add(insertRecord);

                SchemaInfo deleteSchemaInfo = data.cloneSchemaInfo(data.getSchemaInfo());
                CustomData deleteData = new CustomData(deleteSchemaInfo, EventTypeInSdk.DELETE, deleteRecords);

                SchemaInfo insertSchemaInfo = data.cloneSchemaInfo(data.getSchemaInfo());
                CustomData insertData = new CustomData(insertSchemaInfo, EventTypeInSdk.INSERT, insertRecords);

                re.add(deleteData);
                re.add(insertData);
            }
        } else {
            re.add(data);
        }

        return re;
    }

    protected void fillColumnMap(CustomFieldV2 oriBf, LinkedHashMap<String, CustomFieldV2> target) {
        CustomFieldV2 bf = CustomFieldV2.buildField(oriBf.getFieldName(), oriBf.getValue(), oriBf.getSqlType(), oriBf.isKey(), oriBf.isNull(), oriBf.isUpdated());
        target.put(oriBf.getFieldName(), bf);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
