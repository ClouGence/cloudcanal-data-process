package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.*;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 对端对于insert必须使用 Upsert 策略
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToEs_user_2 implements CloudCanalProcessorV2 {

    private DataSource srcDataSource;

    private SchemaInfo factTable = new SchemaInfo(null, "middle_imall", "product_tag_mapping");

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().equals(factTable)) {
            handleFactTable(re, data);
        } else {
            re.add(data);
        }

        return re;
    }

    protected void handleFactTable(List<CustomData> re, CustomData data) {
        String sqlFindProductName = "SELECT `name` FROM `middle_imall`.`product` WHERE `id` = ?";
        String sqlFindTagId = "SELECT `tag_id` FROM `middle_imall`.`product_tag_mapping` WHERE `product_id` = ?";
        String sqlGetTagsPrefix = "SELECT `id`, `name` FROM `middle_imall`.`tag` WHERE `id` in ";
        Iterator<CustomRecordV2> it = data.getRecords().iterator();
        while (it.hasNext()) {
            CustomRecordV2 recordV2 = it.next();

            CustomFieldV2 productId = null;
            switch (data.getEventType()) {
                case INSERT: {
                    productId = recordV2.getAfterColumnMap().get("product_id");
                    break;
                }
                case UPDATE:
                case DELETE: {
                    productId = recordV2.getBeforeColumnMap().get("product_id");
                    break;
                }
                default: {
                    break;
                }
            }

            CustomFieldV2 productIdToRecord = CustomFieldV2.buildField("id", productId.getValue(), productId.getSqlType(), true, false, true);
            CustomFieldV2 productName = null;
            try (Connection conn = srcDataSource.getConnection()) {
                // 获取商品名字
                try (PreparedStatement ps = conn.prepareStatement(sqlFindProductName)) {
                    ps.setObject(1, productId.getValue(), productId.getSqlType());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            String val = rs.getString("name");
                            productName = CustomFieldV2.buildField("name", val, Types.VARCHAR, false, val == null, true);
                        }
                    }
                }

                // 商品为空则删除此条数据
                if (productName == null) {
                    it.remove();
                    continue;
                }

                // 获取tags id
                List<Long> tagIds = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(sqlFindTagId)) {
                    ps.setObject(1, productId.getValue(), productId.getSqlType());

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            tagIds.add(rs.getLong("tag_id"));
                        }
                    }
                }

                // 获取tags
                List<Tag> tags = new ArrayList<>();
                if (!tagIds.isEmpty()) {
                    String idStr = StringUtils.join(tagIds, ",");
                    String sqlGetTags = sqlGetTagsPrefix + "(" + idStr + ")";
                    try (Statement statement = conn.createStatement()) {
                        try (ResultSet rs = statement.executeQuery(sqlGetTags)) {
                            tags = buildTags(rs);
                        }
                    }
                }

                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(tags);

                CustomFieldV2 tagsField = CustomFieldV2.buildField("tags", json, Types.VARCHAR, false, false, true);

                // 生成新数据
                LinkedHashMap<String, CustomFieldV2> d = new LinkedHashMap<>();
                d.put(productIdToRecord.getFieldName(), productIdToRecord);
                d.put(productName.getFieldName(), productName);
                d.put(tagsField.getFieldName(), tagsField);

                LinkedHashMap<String, CustomFieldV2> key = new LinkedHashMap<>();
                key.put(productIdToRecord.getFieldName(), productIdToRecord);

                if (!tags.isEmpty() && data.getEventType() == EventTypeInSdk.DELETE) {
                    // 源端是一条删除，但是实际上商品仍然关联有tag ,则必须做update
                    it.remove();
                    CustomRecordV2 newR = CustomRecordV2.buildRecord(d, d, key, key);
                    List<CustomRecordV2> rs = new ArrayList<>();
                    rs.add(newR);
                    CustomData customData = new CustomData(data.getSchemaInfo(), EventTypeInSdk.UPDATE, rs);
                    re.add(customData);
                } else {
                    recordV2.setAfterColumnMap(d);
                    recordV2.setAfterKeyColumnMap(key);
                    recordV2.setBeforeColumnMap(d);
                    recordV2.setBeforeKeyColumnMap(key);
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }

        re.add(data);
    }

    @Override
    public void stop() {
        // do nothing
    }

    private List<Tag> buildTags(ResultSet rs) throws SQLException {
        List<Tag> tags = new ArrayList<>();
        while (rs.next()) {
            Tag tag = new Tag(rs.getInt("id"), rs.getString("name"));
            tags.add(tag);
        }
        return tags;
    }

    public static class Tag {

        private final long   id;

        private final String name;

        public Tag(long id, String name){
            this.id = id;
            this.name = name;
        }

        public long getId() { return id; }

        public String getName() { return name; }
    }
}
