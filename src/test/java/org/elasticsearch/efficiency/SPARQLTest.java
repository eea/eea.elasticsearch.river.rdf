package org.elasticsearch.efficiency;

import org.elasticsearch.app.ApiServer;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.IndexerController;
import org.elasticsearch.app.api.server.dto.ConfigInfoDTO;
import org.elasticsearch.app.api.server.entities.UpdateRecord;
import org.elasticsearch.app.api.server.entities.UpdateStates;
import org.elasticsearch.app.api.server.exceptions.ConnectionLost;
import org.elasticsearch.app.api.server.services.ConfigManager;
import org.elasticsearch.app.api.server.services.DashboardManager;
import org.junit.*;
import org.junit.Assert.*;
import org.junit.jupiter.api.AfterEach;
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
public class SPARQLTest {

    @Autowired
    IndexerController indexerController;

    @Autowired
    DashboardManager dashboardManager;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

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

    @Test
    public void thousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Thousand line select SPARQL indexing");
        int limit = 1000;
        String indexName = "select-thousand";
        executeSelectQuery(limit, indexName);
        System.out.println("\nTest successful - Thousand line select SPARQL indexing");
    }

    @Test
    public void fiveThousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Five thousand line select SPARQL indexing");
        int limit = 5000;
        String indexName = "select-5thousand";
        executeSelectQuery(limit, indexName);
        System.out.println("\nTest successful - Five thousand line select SPARQL indexing");
    }

    @Test
    public void tenThousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Ten thousand line select SPARQL indexing");
        int limit = 10000;
        String indexName = "select-10thousand";
        executeSelectQuery(limit, indexName);
        System.out.println("\nTest successful - Ten thousand line select SPARQL indexing");
    }

    @Test
    public void twentyThousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Twenty thousand line select SPARQL indexing");
        int limit = 20000;
        String indexName = "select-20thousand";
        executeSelectQuery(limit, indexName);
        System.out.println("\nTest successful - Twenty thousand line select SPARQL indexing");
    }

    private void executeSelectQuery(int limit, String indexName) throws InterruptedException {
        long start = System.currentTimeMillis();
        String config = createQuery(limit, indexName);

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
            TimeUnit.SECONDS.sleep(1);
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
        System.out.printf("\n\nTotal select rows:\t%d rows\n", limit);
        System.out.printf("Total hits:\t\t\t%d hits\n", hits);
        System.out.println("Pipeline:");
        System.out.printf("\tDuration:\t\t\t\t\t\t\t %s (%dms)\n", formatDuration(testDuration), testDuration);
        System.out.printf("\tDuration per select response line:\t %s (%.3fms)\n", formatDuration(testDuration / limit), (double) testDuration / limit);
        System.out.printf("\tDuration per ES hits:\t\t\t\t %s (%.3fms)\n", formatDuration(testDuration / hits), (double) testDuration / hits);
        System.out.println("Indexing:");
        System.out.printf("\tDuration:\t\t\t\t\t\t\t %s (%dms)\n", formatDuration(indexingDuration), indexingDuration);
        System.out.printf("\tDuration per select response line:\t %s (%.3fms)\n", formatDuration(indexingDuration / limit), (double) indexingDuration / limit);
        System.out.printf("\tDuration per ES hits:\t\t\t\t %s (%.3fms)\n", formatDuration(indexingDuration / hits), (double) indexingDuration / hits);
    }

    private String createQuery(int limit, String indexName) {
        String endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
        String query = "\"PREFIX bd: <http://www.bigdata.com/rdf#> PREFIX cc: <http://creativecommons.org/ns#> PREFIX dct: <http://purl.org/dc/terms/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#> PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX p: <http://www.wikidata.org/prop/> PREFIX pq: <http://www.wikidata.org/prop/qualifier/> PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/> PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/> PREFIX pr: <http://www.wikidata.org/prop/reference/> PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/> PREFIX prov: <http://www.w3.org/ns/prov#> PREFIX prv: <http://www.wikidata.org/prop/reference/value/> PREFIX ps: <http://www.wikidata.org/prop/statement/> PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/> PREFIX psv: <http://www.wikidata.org/prop/statement/value/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX schema: <http://schema.org/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/> PREFIX wdno: <http://www.wikidata.org/prop/novalue/> PREFIX wdref: <http://www.wikidata.org/reference/> PREFIX wds: <http://www.wikidata.org/entity/statement/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/> PREFIX wdv: <http://www.wikidata.org/value/> PREFIX wikibase: <http://wikiba.se/ontology#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT       distinct (?disease as ?s) ?p (?symptom_label as ?o) WHERE {       ?disease wdt:P780 ?symptom .        {       ?symptom rdfs:label ?symptom_label . FILTER(lang(?symptom_label) = \\\"en\\\")       SERVICE wikibase:label { bd:serviceParam wikibase:language \\\"en\\\". }       Bind( wdt:P780 as ?p)       } Union{         ?disease rdfs:label ?symptom_label .         FILTER (langMatches( lang(?symptom_label), \\\"EN\\\" ) )         Bind(  rdfs:label as ?p)       } }";
        if (limit != -1)
            query += "LIMIT " + limit;
        query += "\"";
        String config = "{\"schedule\": {\"schedule\": \"0 0 9 * * *\",\"automatic\": false},\"config\": {\"type\": \"eeaRDF\",\"eeaRDF\": {\"endpoint\":\"" + endpoint + "\" ,\"query\": [" + query + "],\"queryType\": \"select\"},\"syncReq\": {},\"index\": {\"index\": \"" + indexName + "\",\"type\": \"rdf\"}}}";
        return config;
    }

    private String formatDuration(long duration) {
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
