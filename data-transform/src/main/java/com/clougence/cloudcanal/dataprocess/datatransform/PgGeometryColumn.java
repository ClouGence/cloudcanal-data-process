package com.clougence.cloudcanal.dataprocess.datatransform;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * 处理 PG geometry 类型，坐标转换
 * 
 * @author mode 2021/11/29 23:07:26
 */
public class PgGeometryColumn implements CloudCanalProcessorV2 {

    private SchemaInfo                   srcTable  = new SchemaInfo("mode_test", "pggis", "td_ccwjq_2020");
    private static final GeometryFactory factory   = new GeometryFactory();
    private final WKTReader              wktReader = new WKTReader(factory);

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        if (data.getSchemaInfo().equals(srcTable)) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                switch (data.getEventType()) {
                    case INSERT: {
                        trimColumnsTime(recordV2, recordV2.getAfterColumnMap());
                        break;
                    }
                    case UPDATE: {
                        trimColumnsTime(recordV2, recordV2.getBeforeColumnMap());
                        trimColumnsTime(recordV2, recordV2.getAfterColumnMap());
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

    protected void trimColumnsTime(CustomRecordV2 recordV2, LinkedHashMap<String, CustomFieldV2> columnMap) {
        // 解析 Geometry WKT 数据
        Geometry parsedData = null;
        CustomFieldV2 gemoColumn = columnMap.get("geom");
        if (gemoColumn != null && gemoColumn.getValue() != null) {
            try {
                parsedData = this.wktReader.read((String) gemoColumn.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        // 获取中心点
        if (parsedData != null) {
            Point centroid = parsedData.getCentroid();
            recordV2.addField(CustomFieldV2.buildField("geom_centroid", (centroid.getX() + "," + centroid.getY()), Types.VARCHAR, false, false, true));
        } else {
            recordV2.addField(CustomFieldV2.buildField("geom_centroid", null, Types.VARCHAR, false, true, true));
        }

        // 获取边界
        if (parsedData != null) {
            Geometry envelope = parsedData.getEnvelope();
            recordV2.addField(CustomFieldV2.buildField("geom_envelope", envelope.toText(), Types.VARCHAR, false, false, true));
        } else {
            recordV2.addField(CustomFieldV2.buildField("geom_envelope", null, Types.VARCHAR, false, true, true));
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
