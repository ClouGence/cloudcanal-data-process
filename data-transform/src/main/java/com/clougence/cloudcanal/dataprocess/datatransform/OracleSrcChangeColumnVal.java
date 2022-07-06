package com.clougence.cloudcanal.dataprocess.datatransform;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 处理 PG geometry 类型，坐标转换
 *
 * @author mode 2021/11/29 23:07:26
 */
public class OracleSrcChangeColumnVal implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    private final SchemaInfo srcTable = new SchemaInfo("orcl", "JUNYU_TEST", "WORKER_STATS");

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        customLogger.info(data.getSchemaInfo().getCatalog() + "." + data.getSchemaInfo().getSchema() + "." + data.getSchemaInfo().getTable() + ",equals:" + data.getSchemaInfo().equals(srcTable));
        if (data.getSchemaInfo().equals(srcTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                switch (data.getEventType()) {
                    case INSERT: {
                        changeColumn(recordV2.getAfterColumnMap());
                        break;
                    }
                    case UPDATE: {
                        changeColumn(recordV2.getBeforeColumnMap());
                        changeColumn(recordV2.getAfterColumnMap());
                        break;
                    }
                    case DELETE: {
                        changeColumn(recordV2.getBeforeColumnMap());
                        break;
                    }
                    default:
                        break;
                }
            }
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    protected void changeColumn(LinkedHashMap<String, CustomFieldV2> columnMap) {
        CustomFieldV2 col = columnMap.get("NEW_COL_COL");
        if (col != null) {
            col.setValue(col.getValue() + "_change_by_my_code");
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
