package org.elasticsearch.efficiency;

import org.elasticsearch.app.ApiServer;
import org.elasticsearch.app.api.server.IndexerController;
import org.elasticsearch.app.api.server.dto.ConfigInfoDTO;
import org.elasticsearch.app.api.server.entities.UpdateRecord;
import org.elasticsearch.app.api.server.entities.UpdateStates;
import org.elasticsearch.app.api.server.exceptions.ConnectionLost;
import org.elasticsearch.app.api.server.services.DashboardManager;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ApiServer.class)
@TestPropertySource(locations = "classpath:application-test.properties")
public abstract class SPARQLTest {

    @Autowired
    IndexerController indexerController;

    @Autowired
    DashboardManager dashboardManager;

    @Before
    public void setUp() throws ConnectionLost {
        System.out.println("Checking connection to ES and Kibana");
        dashboardManager.checkConnection();
        System.out.println("Connection to ES and Kibana - ok");
    }

    @After
    public void afterTest() {
        System.out.println("\n\n----------------------------Deleting indexes from ES----------------------------\n\n");
        indexerController.getConfigs().forEach(c -> indexerController.deleteIndex(c.getName(), true));
    }


    void executeSPARQLQuery(String query, String endpoint, String queryType, int limit, String indexName) throws InterruptedException {
        long start = System.currentTimeMillis();
        String config = createQuery(query, endpoint, queryType, limit, indexName);

        System.out.println("Saving config");
        indexerController.saveConfig(config);
        Assert.assertNotNull(indexerController.getConfig(indexName));
        System.out.println("Starting config indexing");
        indexerController.startIndex(indexName);
        TimeUnit.MILLISECONDS.sleep(100);
        Assert.assertTrue(indexerController.runningHarvests().containsKey(indexName));
        System.out.println("Indexing is running");

        //Simulating frontend requests
        while (true) {
            TimeUnit.MILLISECONDS.sleep(10);
            indexerController.runningHarvests();
            if (!indexerController.runningHarvests().containsKey(indexName))
                break;
        }
        System.out.println("Indexed");
        System.out.println("Getting update record");
        ConfigInfoDTO found = indexerController.getConfigs().stream().filter(c -> c.getName().equals(indexName)).findFirst().orElse(null);
        Assert.assertNotNull(found);
        UpdateRecord updateRecord = found.getLastSuccessAndLastTenUpdateRecords().get(1);
        Assert.assertNotNull(updateRecord);
        Assert.assertEquals(updateRecord.getFinishState(), UpdateStates.SUCCESS);
        System.out.println("Indexing Successful");

        long indexingDuration = updateRecord.getLastUpdateDuration();
        long testDuration = System.currentTimeMillis() - start;
        long hits = updateRecord.getIndexedESHits();
        System.out.printf("\n\nTotal select rows: %d rows\n", limit);
        System.out.printf("Total hits: %d hits\n", hits);
        System.out.println("Pipeline:");
        System.out.printf("\tDuration: %s (%dms)\n", formatDuration(testDuration), testDuration);
        System.out.printf("\tDuration per select response line: %s (%.3fms)\n", formatDuration(testDuration / limit), (double) testDuration / limit);
        System.out.printf("\tDuration per ES hits: %s (%.3fms)\n", formatDuration(testDuration / hits), (double) testDuration / hits);
        System.out.println("Harvesting and indexing:");
        System.out.printf("\tDuration: %s (%dms)\n", formatDuration(indexingDuration), indexingDuration);
        System.out.printf("\tDuration per select response line: %s (%.3fms)\n", formatDuration(indexingDuration / limit), (double) indexingDuration / limit);
        System.out.printf("\tDuration per ES hits: %s (%.3fms)\n", formatDuration(indexingDuration / hits), (double) indexingDuration / hits);
    }

    String createQuery(String query, String endpoint, String queryType, int limit, String indexName) {
        if (limit != -1)
            query += "LIMIT " + limit;
        String config = "{\"schedule\": {\"schedule\": \"0 0 9 * * *\",\"automatic\": false},\"config\": {\"type\": \"eeaRDF\",\"eeaRDF\": {\"endpoint\":\"" + endpoint + "\" ,\"query\": [\"" + query + "\"],\"queryType\": \""+queryType+"\"},\"syncReq\": {},\"index\": {\"index\": \"" + indexName + "\",\"type\": \"rdf\"}}}";
        return config;
    }

    String formatDuration(long duration) {
        String time = duration % 1000 + "ms";
        if ((duration /= 1000) > 0)
            time = duration % 60 + "s " + time;
        if ((duration /= 60) > 0)
            time = duration % 60 + "m " + time;
        if ((duration /= 60) > 0)
            time = duration % 24 + "h " + time;
        if ((duration /= 24) > 0)
            time = duration + "days " + time;
        return time;
    }

}
