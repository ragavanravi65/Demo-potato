//package com.exampleBQ.demoBQ;
//
//
//import com.google.cloud.bigquery.BigQuery;
//import com.google.cloud.bigquery.QueryJobConfiguration;
//import com.google.cloud.bigquery.TableId;
//import com.google.cloud.bigquery.TableResult;
//import org.assertj.core.api.Assertions;
//import org.junit.*;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.web.client.TestRestTemplate;
//import org.springframework.http.HttpEntity;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.MediaType;
//import org.springframework.http.ResponseEntity;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.util.LinkedMultiValueMap;
//
//import javax.annotation.Resource;
//import java.io.IOException;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.StreamSupport;
//
//@RunWith(SpringRunner.class)
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
//        classes = DemoBqApplication.class, properties = "spring.cloud.gcp.bigquery.datasetName=DevWorks")
//public class BQIntegrationTest {
//
//
//    private static final String DATASET_NAME = "DevWorks";
//
//    private static final String TABLE_NAME = "GenderTest";
//
//    @Autowired
//    BigQuery bigQuery;
//
//    @BeforeClass
//    public static void prepare() {
////        Assume.assumeThat(
////                "BigQuery integration tests are disabled. "
////                        + "Please use '-Dit.bigquery=true' to enable them.",
////                System.getProperty("it.bigquery"), is("true"));
//    }
//
//    @Before
//    @After
//    public void cleanupTestEnvironment() {
//        // Clear the previous dataset before beginning the test.
//        this.bigQuery.delete(TableId.of(DATASET_NAME, TABLE_NAME));
//    }
//
//    @Test
//    public void testFileUpload() throws InterruptedException, IOException {
//        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
//        map.add("file", csvFile);
//        map.add("tableName", TABLE_NAME);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
//
//        HttpEntity<LinkedMultiValueMap<String, Object>> request = new HttpEntity<>(map, headers);
//        ResponseEntity<String> response =
//                this.restTemplate.postForEntity("/uploadFile", request, String.class);
//        Assertions.assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
//
//        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration
//                .newBuilder("SELECT * FROM " + DATASET_NAME + "." + TABLE_NAME)
//                .build();
//
//        TableResult queryResult = this.bigQuery.query(queryJobConfiguration);
//        Assertions.assertThat(queryResult.getTotalRows()).isEqualTo(3);
//
//        List<String> names = StreamSupport.stream(queryResult.getValues().spliterator(), false)
//                .map(valueList -> valueList.get(0).getStringValue())
//                .collect(Collectors.toList());
//        Assertions.assertThat(names).containsExactlyInAnyOrder("Nathaniel", "Diaz", "Johnson");
//    }
//
//}
