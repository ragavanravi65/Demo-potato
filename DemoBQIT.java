package com.exampleBQ.demoBQ;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DemoBQIT {

    private BigQuery bigquery;
    private String projectId;
    private String dataset;
    private TableId tableId;

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
        String table = "gcpPOCTest";
        String tableSpec = String.format("%s.%s", dataset, table);
        tableId = TableId.of(dataset, table);
        System.out.println(tableId);
        bigquery.create(DatasetInfo.newBuilder(dataset).build());

        bigquery
                .create(TableInfo
                        .newBuilder(tableId,
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
        row2Data.put("Name", "rrr");

        InsertAllResponse response =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .addRow("row1Id", row1Data)
                                .addRow("row2Id", row2Data)
                                .build());

        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            System.out.println("error while saving to bq");
        }
    }

    @After
    public void deleteBigQueryDataset() {
        RemoteBigQueryHelper.forceDelete(bigquery, dataset);
    }
    @Test
    public void testBq() throws Exception{
        System.out.println("testing");
//        String finalQuery="Select * from " +dataset+"."+tab;
//        QueryJobConfiguration queryConfig =
//                QueryJobConfiguration.newBuilder(finalQuery).build();
//
//        TableResult queryJob = bigquery.query(queryConfig);
    }
}