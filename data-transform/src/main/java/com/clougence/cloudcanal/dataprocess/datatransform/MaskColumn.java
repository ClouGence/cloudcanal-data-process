package com.clougence.cloudcanal.dataprocess.datatransform;

import java.util.*;

import org.apache.commons.lang3.StringUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * full/part data mask.
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class MaskColumn implements CloudCanalProcessorV2 {

    private static final char                   MASK_CHAR = '*';
    private final Map<SchemaInfo, List<String>> colMap    = new HashMap<>();

    @Override
    public void start(ProcessorContext context) {
        colMap.put(new SchemaInfo(null, "dingtax", "user_info"), Arrays.asList("phone", "password", "sk"));
        colMap.put(new SchemaInfo(null, "dingtax", "ds_info"), Arrays.asList("password", "secret_key"));
    }

    /**
     * the code not mask key columns cause data update/delete maybe failed, if need , put them to mask carefully.
     */
    @Override
    public List<CustomData> process(CustomData data) {
        List<String> cols = colMap.get(data.getSchemaInfo());

        if (cols != null) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                for (String col : cols) {
                    switch (data.getEventType()) {
                        case INSERT: {
                            CustomFieldV2 v1 = recordV2.getAfterColumnMap().get(col);
                            if (v1 != null) {
                                fullMask(v1);
                            }

                            break;
                        }
                        case UPDATE: {
                            CustomFieldV2 v1 = recordV2.getAfterColumnMap().get(col);
                            if (v1 != null) {
                                fullMask(v1);
                            }

                            CustomFieldV2 v2 = recordV2.getBeforeColumnMap().get(col);
                            if (v2 != null) {
                                fullMask(v2);
                            }

                            break;
                        }
                        case DELETE: {
                            CustomFieldV2 v2 = recordV2.getBeforeColumnMap().get(col);
                            if (v2 != null) {
                                fullMask(v2);
                            }
                        }
                        default:
                            break;
                    }
                }
            }
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    protected void fullMask(CustomFieldV2 f) {
        if (f.getValue() == null || f.isNull()) {
            return;
        }

        String val;
        if (f.getValue() instanceof String) {
            val = (String) (f.getValue());
        } else {
            val = String.valueOf(f.getValue());
        }

        if (val.equals("")) {
            return;
        }

        String maskedVal = StringUtils.repeat(MASK_CHAR, val.length());
        f.setValue(maskedVal);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
