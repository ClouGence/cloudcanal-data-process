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
public class MySqlToEsNoDelete implements CloudCanalProcessorV2 {

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getEventType() == EventTypeInSdk.DELETE) {
            List<CustomRecordV2> updateRecords = new ArrayList<>();
            for (CustomRecordV2 oriRecord : data.getRecords()) {
                CustomRecordV2 updateRecord = new CustomRecordV2();
                updateRecord.setBeforeColumnMap(oriRecord.getBeforeColumnMap());
                updateRecord.setBeforeKeyColumnMap(oriRecord.getBeforeKeyColumnMap());
                for (Map.Entry<String, CustomFieldV2> afterField : oriRecord.getAfterColumnMap().entrySet()) {
                    afterField.getValue().setValue(null);
                    afterField.getValue().setNull(true);
                    afterField.getValue().setUpdated(true);

                    updateRecord.getAfterColumnMap().put(afterField.getKey(), afterField.getValue());
                    if (afterField.getValue().isKey()) {
                        updateRecord.getAfterKeyColumnMap().put(afterField.getKey(), afterField.getValue());
                    }
                }

                updateRecords.add(updateRecord);
            }

            CustomData updateData = new CustomData(data.getSchemaInfo(), EventTypeInSdk.UPDATE, updateRecords);
            re.add(updateData);
        } else {
            re.add(data);
        }

        return re;
    }

    protected SchemaInfo cloneSchemaInfo(SchemaInfo oriSchema) {
        return new SchemaInfo(oriSchema.getCatalog(), oriSchema.getSchema(), oriSchema.getTable());
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
