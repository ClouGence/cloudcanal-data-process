package com.clougence.cloudcanal.dataprocess.datatransform;

import java.sql.Types;
import java.util.*;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * @author bucketli 2022/11/3 11:40:03
 */
public class VwDataUpgrade implements CloudCanalProcessorV2 {

    public static final String    DATA_SOURCE_FIELD = "data_source";

    public static final String    FTB2_0            = "FTB2.0";

    protected static final Logger licenseLogger     = LoggerFactory.getLogger("Mosc4LicenseDetailToLicense");

    private final SchemaInfo      targetTable       = new SchemaInfo(null, "dingtax", "LICENSE_DETAIL");

    private DataSource            srcDataTarget;

    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataTarget = (DataSource) context.getDstRdbDs();
    }

    @Override
    public List<CustomData> process(CustomData data) {
        licenseLogger.info("### start Mosc4LicenseDetailToLicense process......");
        List<CustomData> re = new ArrayList<>();
        List<CustomRecordV2> newRecords = new ArrayList<>();
        if (data.getSchemaInfo().equals(targetTable)) {
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        if (changeStatusColumnValue(recordV2.getAfterColumnMap())) {
                            newRecords.add(recordV2);
                        }
                    }
                    break;
                }
                case DELETE: {
                    for (CustomRecordV2 recordV2 : data.getRecords()) {
                        if (changeStatusColumnValue(recordV2.getBeforeColumnMap())) {
                            newRecords.add(recordV2);
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        CustomData newData = new CustomData(targetTable, data.getEventType(), newRecords);
        re.add(newData);
        return re;
    }

    protected boolean changeStatusColumnValue(LinkedHashMap<String, CustomFieldV2> columns) {
        CustomFieldV2 dataSource = CustomFieldV2.buildField(DATA_SOURCE_FIELD, FTB2_0, Types.VARCHAR, false, false, true);
        columns.put(dataSource.getFieldName(), dataSource);

        Object vinObject = columns.get("vin").getValue();
        String vin = vinObject.toString();
        if (StringUtils.isBlank(vin)) {
            return false;
        }
        // 统一设置品牌：VW
        CustomFieldV2 brandCodeField = CustomFieldV2.buildField("brand_code", "VW", Types.VARCHAR, false, false, true);
        columns.put(brandCodeField.getFieldName(), brandCodeField);
        // 统一设置license类型：licenseType
        CustomFieldV2 licenseTypeField = CustomFieldV2.buildField("license_type", "PCM", Types.VARCHAR, false, false, true);
        columns.put(licenseTypeField.getFieldName(), licenseTypeField);

        String date = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
        CustomFieldV2 updateTimeField = CustomFieldV2.buildField("update_time", date, Types.DATE, false, false, true);
        columns.put(updateTimeField.getFieldName(), updateTimeField);

        // licenseType -> use_type
        Object userTypeObj = columns.get("licenseType").getValue();
        String userType = "";
        if (userTypeObj != null) {
            userType = userTypeObj.toString();
            if ("DEALER".equals(userType)) {
                userType = "SHOW_VEHICLE";
            }
        }
        columns.get("licenseType").setValue(userType);

        // startTime -> start_time
        Object startTime = columns.get("startTime").getValue();
        Date date1 = new Date(Long.parseLong(startTime.toString()));
        columns.get("startTime").setSqlType(Types.VARCHAR);
        columns.get("startTime").setValue(DateFormatUtils.format(date1, "yyyy-MM-dd HH:mm:ss"));

        // endTime -> end_time
        Object endTime = columns.get("endTime").getValue();
        Date date2 = new Date(Long.parseLong(endTime.toString()));
        columns.get("endTime").setSqlType(Types.VARCHAR);
        columns.get("endTime").setValue(DateFormatUtils.format(date2, "yyyy-MM-dd HH:mm:ss"));

        Object licenseStatus = columns.get("licenseStatus").getValue();
        if (Objects.nonNull(licenseStatus)) {
            columns.get("licenseStatus").setSqlType(Types.VARCHAR);
            String status = LicenseStatus.getName(licenseStatus.toString());
            if (Objects.nonNull(status)) {
                columns.get("licenseStatus").setUpdated(true);
                columns.get("licenseStatus").setValue(status);
            }
        }

        CustomFieldV2 platformField = CustomFieldV2.buildField("platform", "MOSC3.1", Types.VARCHAR, false, false, true);
        columns.put(platformField.getFieldName(), platformField);

        return true;
    }

    @Override
    public void stop() {
        // do nothing
    }

    public enum LicenseStatus {

        TO_OPEN("0", "待开通"),
        OPENED("1", "开通成功"),
        TO_EXPIRE("2", "待过期"),
        EXPIRED("3", "已过期"),
        TO_CANCEL("4", "待取消"),
        CANCELED("5", "已取消");

        private String code;

        private String desc;

        LicenseStatus(String code, String desc){
            this.code = code;
            this.desc = desc;
        }

        public String getCode() { return code; }

        public void setCode(String code) { this.code = code; }

        public String getDesc() { return desc; }

        public void setDesc(String desc) { this.desc = desc; }

        public static String getName(String code) {
            for (LicenseStatus licenseStatus : LicenseStatus.values()) {
                if (licenseStatus.code.equals(code)) {
                    return licenseStatus.name();
                }
            }
            return null;
        }
    }
}
