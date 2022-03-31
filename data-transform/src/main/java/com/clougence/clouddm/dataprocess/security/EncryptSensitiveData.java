package com.clougence.clouddm.dataprocess.security;

import org.apache.commons.lang3.StringUtils;

import com.clougence.clouddm.sdk.api.ColMetaDataInSdk;
import com.clougence.clouddm.sdk.api.ColProcess;

/**
 * @author bucketli 2022/3/30 19:35:53
 */
public class EncryptSensitiveData implements ColProcess {

    @Override
    public Object doProcess(Object colValue, ColMetaDataInSdk colMeta, String processConfig) {
        if (colValue == null) {
            return null;
        }

        String strVal = String.valueOf(colValue);
        return StringUtils.repeat("*", strVal.length());
    }
}
