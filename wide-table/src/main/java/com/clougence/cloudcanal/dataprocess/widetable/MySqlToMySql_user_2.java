package com.clougence.cloudcanal.dataprocess.widetable;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToMySql_user_2 implements CloudCanalProcessorV2 {

    private DataSource srcDataSource;

    private SchemaInfo factTable = new SchemaInfo(null, "middle_imall", "product");

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
        String sqlFindTagId = "SELECT `tag_id` FROM `middle_imall`.`product_tag_mapping` WHERE `product_id` = ?";
        String sqlGetTagsPrefix = "SELECT `id`, `name` FROM `middle_imall`.`tag` WHERE `id` in ";
        for (CustomRecordV2 recordV2 : data.getRecords()) {
            try (Connection conn = srcDataSource.getConnection()) {
                List<Long> tagIds = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(sqlFindTagId)) {
                    switch (data.getEventType()) {
                        case INSERT: {
                            ps.setObject(1, recordV2.getAfterColumnMap().get("id").getValue(), Types.BIGINT);
                            break;
                        }
                        case UPDATE:
                        case DELETE: {
                            ps.setObject(1, recordV2.getBeforeColumnMap().get("id").getValue(), Types.BIGINT);
                            break;
                        }
                        default: {
                            break;
                        }
                    }

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            tagIds.add(rs.getLong("tag_id"));
                        }
                    }
                }

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
                recordV2.addField(CustomFieldV2.buildField("tags", json, Types.VARCHAR, false, false, true));
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
