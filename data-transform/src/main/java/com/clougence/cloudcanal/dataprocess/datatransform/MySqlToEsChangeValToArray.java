package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToEsChangeValToArray implements CloudCanalProcessorV2 {

    private SchemaInfo srcTable = new SchemaInfo(null, "es_array_db", "array_test");

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        if (data.getSchemaInfo().equals(srcTable)) {
            changeValToArray(data);
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    protected List<Object> genVarList(Object val) {
        List<Object> vals = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            vals.add(val);
        }

        return vals;
    }

    protected void changeVal(LinkedHashMap<String, CustomFieldV2> valMap) {
        CustomFieldV2 varField = valMap.get("var_array");
        varField.setSqlType(Types.ARRAY);
        varField.setValue(genVarList(varField.getValue()));

        CustomFieldV2 numField = valMap.get("num_array");
        numField.setSqlType(Types.ARRAY);
        String strVal = numField.getValue().toString();
        numField.setValue(genVarList(Long.valueOf(strVal)));
    }

    protected void changeValToArray(CustomData data) {
        for (CustomRecordV2 oriRecord : data.getRecords()) {
            switch (data.getEventType()) {
                case INSERT: {
                    changeVal(oriRecord.getAfterColumnMap());
                    break;
                }
                case UPDATE: {
                    changeVal(oriRecord.getAfterColumnMap());
                    changeVal(oriRecord.getBeforeColumnMap());
                }
                case DELETE: {
                    changeVal(oriRecord.getBeforeColumnMap());
                }
                default:
                    throw new IllegalArgumentException("unsupported event type:" + data.getEventType());
            }
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
