package com.clougence.cloudcanal.dataprocess.widetable;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 纯粹打数据日志
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class DoNothingProcessor implements CloudCanalProcessorV2 {

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
