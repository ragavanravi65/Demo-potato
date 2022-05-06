package com.exampleBQ.demoBQ;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DemoBQIT {

    private BigQuery bigquery;
    private String projectId;
    private String dataset;
    private TableId tableId_0;
    private TableId tableId_1;

    @Autowired
    BQController bqController;

    @Autowired
    BQService bqService;
//
//
//    @Autowired
//    private WebTestClient webTestClient;

    /** Find credentials in the environment and create a dataset in BigQuery. */
//    @Before
//    public void createBigQueryDataset() {
//        RemoteBigQueryHelper bqHelper = RemoteBigQueryHelper.create();
//        bigquery = bqHelper.getOptions().getService();
//        projectId = bqHelper.getOptions().getProjectId();
//        dataset = RemoteBigQueryHelper.generateDatasetName();
//    }
//    @BeforeClass
//    public static void beforeClass() throws IOException {
//        final Map<PropertyDescriptor, String> propertiesMap = new HashMap<>();
//        propertiesMap.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE, SERVICE_ACCOUNT_JSON);
//        Credentials credentials = credentialsProviderFactory.getGoogleCredentials(propertiesMap, new ProxyAwareTransportFactory(null));
//
//        BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
//                .setProjectId(PROJECT_ID)
//                .setCredentials(credentials)
//                .build();
//
//        bigquery = bigQueryOptions.getService();
//
//        DatasetInfo datasetInfo = DatasetInfo.newBuilder(RemoteBigQueryHelper.generateDatasetName()).build();
//        dataset = bigquery.create(datasetInfo);
//    }

    @Before
    public void defineMyBigQuery() {
        RemoteBigQueryHelper bqHelper = RemoteBigQueryHelper.create();
        bigquery = bqHelper.getOptions().getService();
        projectId = bqHelper.getOptions().getProjectId();
        dataset = RemoteBigQueryHelper.generateDatasetName();
        String table_0 = "GenderData";
        String table_1 = "GenderAbb";
//        String tableSpec = String.format("%s.%s", dataset, table_0);
        tableId_0 = TableId.of(dataset, table_0);
        tableId_1 = TableId.of(dataset, table_1);
        System.out.println(tableId_0);
        System.out.println(tableId_1);
        bigquery.create(DatasetInfo.newBuilder(dataset).build());

        InsertAllResponse responset0=createSourcetable(bigquery,tableId_0);
        InsertAllResponse responset1=createSecondTable(bigquery,tableId_1);
        if (responset0.hasErrors() || responset1.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            System.out.println("error while saving to bq");
        }
    }

    private InsertAllResponse createSourcetable(BigQuery bigquery, TableId tableId_0) {
        bigquery
                .create(TableInfo
                        .newBuilder(tableId_0,
                                StandardTableDefinition
                                        .of(Schema.of(Field.of("Gender", LegacySQLTypeName.STRING),
                                                Field.of("Name", LegacySQLTypeName.STRING)))
                                        .toBuilder()
//                                        .setTimePartitioning(TIME_PARTITIONING)
//                                        .setClustering(CLUSTERING)
                                        .build())
                        .build());

        //populate data to my tables

        Map<String, Object> row1Data = new HashMap<>();
        row1Data.put("Gender", "M");
        row1Data.put("Name", "Ragav");

        Map<String, Object> row2Data = new HashMap<>();
        row2Data.put("Gender", "F");
        row2Data.put("Name", "shree");

        InsertAllResponse response =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId_0)
                                .addRow("row1Id", row1Data)
                                .addRow("row2Id", row2Data)
                                .build());

        return response;

    }
    private InsertAllResponse createSecondTable(BigQuery bigquery, TableId tableId_1) {
        bigquery
                .create(TableInfo
                        .newBuilder(tableId_1,
                                StandardTableDefinition
                                        .of(Schema.of(Field.of("Type", LegacySQLTypeName.STRING),
                                                Field.of("Description", LegacySQLTypeName.STRING)))
                                        .toBuilder()
//                                        .setTimePartitioning(TIME_PARTITIONING)
//                                        .setClustering(CLUSTERING)
                                        .build())
                        .build());

        //populate data to my tables

        Map<String, Object> row1Data = new HashMap<>();
        row1Data.put("Type", "M");
        row1Data.put("Description", "MALE");

        Map<String, Object> row2Data = new HashMap<>();
        row2Data.put("Type", "F");
        row2Data.put("Description", "FEMALE");

        InsertAllResponse response1 =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId_1)
                                .addRow("row1Id", row1Data)
                                .addRow("row2Id", row2Data)
                                .build());

        return response1;

    }
    @After
    public void deleteBigQueryDataset() {
//        RemoteBigQueryHelper.forceDelete(bigquery, dataset);
    }
    @Test
    public void testBq() throws Exception{
        ReflectionTestUtils.setField(bqService,"datasetName",dataset);
        System.out.println("testing");
        System.out.println(bqController.getValue());
    }
}
