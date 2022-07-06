package com.clougence.cloudcanal.dataprocess.widetable;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.JavaDsType;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bucketli 2022/6/8 13:45:08
 */
public class MySqlToMySqlNucleicAcid implements CloudCanalProcessorV2 {

    private DataSource srcDataSource;

    private final SchemaInfo nucleic_acid = new SchemaInfo(null, "rcp", "nucleic_acid");

    private final SchemaInfo rcp_test_result = new SchemaInfo(null, "rcp", "rcp_test_result");

    private final SchemaInfo rcp_sap_tube_state = new SchemaInfo(null, "rcp", "rcp_sap_tube_state");


    @Override
    public void start(ProcessorContext context) {
        if (context.getSrcDsType().getContextDsType() != JavaDsType.JdbcDataSource) {
            throw new IllegalArgumentException("src ds type is not JdbcDataSource");
        }

        srcDataSource = (DataSource) context.getSrcRdbDs();
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> result;
        SchemaInfo input = data.getSchemaInfo();
        if (input.getSchema().equals(rcp_test_result.getSchema()) && input.getTable().equals(rcp_test_result.getTable())) {
            result = handleRcpTestResult(data);
        } else if (input.getSchema().equals(rcp_sap_tube_state.getSchema()) && input.getTable().equals(rcp_sap_tube_state.getTable())) {
            result = handleRcpSapTubeState(data);
        } else {
            result = new ArrayList<>();
            result.add(data);
        }

        return result;
    }

    private static final String rcpTestReportSql = "select `idenno`,`patient_name`,`patiet_mobile` from rcp.rcp_test_report where id =?";

    protected List<CustomData> handleRcpTestResult(CustomData data) {
        List<CustomData> re = new ArrayList<>();

        for (CustomRecordV2 record : data.getRecords()) {
            CustomRecordV2 customRecordV2 = new CustomRecordV2();
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = record.getAfterColumnMap().get("report_id");
                    break;
                default:
                    continue;
            }

            try (Connection conn = srcDataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(rcpTestReportSql)) {
                ps.setObject(1, f.getValue(), Types.VARCHAR);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String idCardNo = rs.getString("id_card_no");
                        String patientName = rs.getString("patient_name");
                        String mobile = rs.getString("mobile");

                        customRecordV2.addField(CustomFieldV2.buildField("id_card_no", idCardNo, Types.VARCHAR, false, idCardNo == null, true));
                        customRecordV2.addField(CustomFieldV2.buildField("patientName", patientName, Types.VARCHAR, false, patientName == null, true));
                        customRecordV2.addField(CustomFieldV2.buildField("mobile", mobile, Types.VARCHAR, false, mobile == null, true));

                        Object result = record.getAfterColumnMap().get("result").getValue();
                        Object result_time = record.getAfterColumnMap().get("result_time").getValue();
                        if (result != null) {
                            if ("阴性".equals(result)) {
                                customRecordV2.addField(CustomFieldV2.buildField("state", 0, Types.TINYINT, false, mobile == null, true));
                            } else if ("阳性".equals(result)) {
                                customRecordV2.addField(CustomFieldV2.buildField("state", 1, Types.TINYINT, false, mobile == null, true));
                            } else {
                                continue;
                            }
                            customRecordV2.addField(CustomFieldV2.buildField("approve_time", result_time, Types.DATE, false, mobile == null, true));
                            List<CustomRecordV2> arrayList = new ArrayList<>();
                            arrayList.add(customRecordV2);
                            CustomData newD = new CustomData(nucleic_acid, EventTypeInSdk.INSERT, arrayList);
                            re.add(newD);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }

        return re;
    }

    private static final String tubePatientSql = "select `patient_id` from rcp.rcp_tube_patient where `tube_id` =?";
    private static final String patientInfoSql = "select `idenno`,`patient_name`,`mobile` from rcp.rcp_patient_info where `patient_id` =?";

    protected List<CustomData> handleRcpSapTubeState(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        for (CustomRecordV2 record : data.getRecords()) {
            CustomFieldV2 f;
            switch (data.getEventType()) {
                case INSERT:
                case UPDATE:
                    f = record.getAfterColumnMap().get("tube_id");
                    break;
                default:
                    continue;
            }

            try (Connection rcp_tube_patient_conn = srcDataSource.getConnection(); PreparedStatement rcp_tube_patient_ps = rcp_tube_patient_conn.prepareStatement(tubePatientSql)) {
                rcp_tube_patient_ps.setObject(1, f.getValue(), Types.VARCHAR);
                try (ResultSet rcp_tube_patient_rs = rcp_tube_patient_ps.executeQuery()) {
                    while (rcp_tube_patient_rs.next()) {
                        CustomRecordV2 customRecordV2 = new CustomRecordV2();
                        String patientId = rcp_tube_patient_rs.getString("patient_id");

                        if (StringUtils.isNotEmpty(patientId)) {
                            try (Connection rcp_patient_info_conn = srcDataSource.getConnection(); PreparedStatement rcp_patient_info_ps = rcp_patient_info_conn.prepareStatement(patientInfoSql)) {
                                rcp_patient_info_ps.setObject(1, patientId, Types.VARCHAR);
                                ResultSet rcp_patient_info_rs = rcp_patient_info_ps.executeQuery();
                                if (rcp_patient_info_rs.next()) {
                                    String idCardNo = rcp_patient_info_rs.getString("idenno");
                                    String patientName = rcp_patient_info_rs.getString("patient_name");
                                    String mobile = rcp_patient_info_rs.getString("mobile");

                                    customRecordV2.addField(CustomFieldV2.buildField("id_card_no", idCardNo, Types.VARCHAR, false, idCardNo == null, true));
                                    customRecordV2.addField(CustomFieldV2.buildField("patientName", patientName, Types.VARCHAR, false, patientName == null, true));
                                    customRecordV2.addField(CustomFieldV2.buildField("mobile", mobile, Types.VARCHAR, false, mobile == null, true));

                                    Object result = record.getAfterColumnMap().get("state").getValue();
                                    Object result_time = record.getAfterColumnMap().get("approve_time").getValue();
                                    if (result != null) {
                                        if ("negative".equals(result)) {
                                            customRecordV2.addField(CustomFieldV2.buildField("state", 0, Types.TINYINT, false, mobile == null, true));
                                        } else if ("positive".equals(result)) {
                                            customRecordV2.addField(CustomFieldV2.buildField("state", 1, Types.TINYINT, false, mobile == null, true));
                                        } else {
                                            continue;
                                        }
                                        customRecordV2.addField(CustomFieldV2.buildField("approve_time", result_time, Types.DATE, false, mobile == null, true));
                                        List<CustomRecordV2> arrayList = new ArrayList();
                                        arrayList.add(customRecordV2);
                                        CustomData newD = new CustomData(nucleic_acid, EventTypeInSdk.INSERT, arrayList);
                                        re.add(newD);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("process error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            }
        }

        return re;
    }


    @Override
    public void stop() {
        // do nothing
    }
}
