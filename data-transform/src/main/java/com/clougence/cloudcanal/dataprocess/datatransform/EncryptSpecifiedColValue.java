package com.clougence.cloudcanal.dataprocess.datatransform;

import com.alibaba.druid.util.FnvHash;
import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bucketli 2022/2/23 17:59:25
 */
public class EncryptSpecifiedColValue implements CloudCanalProcessorV2 {

    protected static final Logger log = LoggerFactory.getLogger("custom_processor");

    private final SchemaInfo srcTable = new SchemaInfo("dingtax", "dingtax", "worker_stats", "my-038krddt737oxx9.dingtax.worker_stats");

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public void stop() {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();

        if (data.getSchemaInfo().equals(srcTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                if (recordV2.getAfterColumnMap() != null) {
                    CustomFieldV2 workerIdF = recordV2.getAfterColumnMap().get("worker_id");
                    if (workerIdF != null) {
                        encryptValue(workerIdF);
                    }
                }

                if (recordV2.getBeforeColumnMap() != null) {
                    CustomFieldV2 workerIdF = recordV2.getBeforeColumnMap().get("worker_id");
                    if (workerIdF != null) {
                        encryptValue(workerIdF);
                    }
                }

                if (recordV2.getBeforeKeyColumnMap() != null) {
                    CustomFieldV2 workerIdF = recordV2.getBeforeKeyColumnMap().get("worker_id");
                    if (workerIdF != null) {
                        encryptValue(workerIdF);
                    }
                }

                if (recordV2.getAfterKeyColumnMap() != null) {
                    CustomFieldV2 workerIdF = recordV2.getAfterKeyColumnMap().get("worker_id");
                    if (workerIdF != null) {
                        encryptValue(workerIdF);
                    }
                }
            }

            re.add(data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected void encryptValue(CustomFieldV2 workerIdF) {
        if (workerIdF.isNull()) {
            return;
        }

        String num = "" + FnvHash.fnv1a_64(String.valueOf(workerIdF.getValue()));
        workerIdF.setValue(num);
        workerIdF.setUpdated(true);
    }
}
