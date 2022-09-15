package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

import java.util.*;

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
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                switch (data.getEventType()) {
                    case INSERT: {
                        for (String f : recordV2.getAfterColumnMap().keySet()) {
                            if (!keepCols.contains(f)) {
                                recordV2.dropField(f);
                            }
                        }
                        break;
                    }
                    case UPDATE:
                    case DELETE: {
                        for (String f : recordV2.getBeforeColumnMap().keySet()) {
                            if (!keepCols.contains(f)) {
                                recordV2.dropField(f);
                            }
                        }
                        break;
                    }
                    default:
                        break;
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
