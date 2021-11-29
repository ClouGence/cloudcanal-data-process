package com.clougence.cloudcanal.dataprocess.widetable;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessor;
import com.clougence.cloudcanal.sdk.api.model.CustomProcessorContext;
import com.clougence.cloudcanal.sdk.api.model.CustomRecord;

import java.util.List;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class OneFactTableOneDimensionTableProcessor implements CloudCanalProcessor {

    @Override
    public List<CustomRecord> process(List<CustomRecord> records, CustomProcessorContext context) {
        return null;
    }
}
