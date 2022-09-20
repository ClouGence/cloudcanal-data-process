package com.clougence.cloudcanal.dataprocess.datatransform;

import java.util.*;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MongoSrcKeepColumns implements CloudCanalProcessorV2 {

    private final SchemaInfo  targetTable = new SchemaInfo(null, "ecall_fasteroms_ods", "OrderInfo");

    private final Set<String> keepCols    = new HashSet<>(Arrays.asList("_id", "orderCodeOfSaleChannel", "payTime", "storeAndOrderUniMap", "storeCode", "storeName", "orderLines"));

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            if (data.getRecords() != null && !data.getRecords().isEmpty()) {
                CustomRecordV2 sample = data.getRecords().get(0);
                List<String> needRemove = new ArrayList<>();

                switch (data.getEventType()) {
                    case INSERT: {
                        for (Map.Entry<String, CustomFieldV2> f : sample.getAfterColumnMap().entrySet()) {
                            if (!keepCols.contains(f.getKey())) {
                                needRemove.add(f.getKey());
                            }
                        }
                        break;
                    }
                    case UPDATE:
                    case DELETE: {
                        for (Map.Entry<String, CustomFieldV2> f : sample.getBeforeColumnMap().entrySet()) {
                            if (!keepCols.contains(f.getKey())) {
                                needRemove.add(f.getKey());
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }

                if (!needRemove.isEmpty()) {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        for (String f : needRemove) {
                            recordV2.dropField(f);
                        }
                    }
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
