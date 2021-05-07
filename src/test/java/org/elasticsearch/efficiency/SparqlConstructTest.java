package org.elasticsearch.efficiency;

import org.elasticsearch.app.ApiServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ApiServer.class)
@TestPropertySource(locations = "classpath:application-test.properties")
public class SparqlConstructTest extends SPARQLTest {
    String endpoint = "https://xn--slovnk-7va.gov.cz/sparql";
    String query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
    String queryType = "construct";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(3600);

    @Test
    public void thousandConstructSPARQL() throws InterruptedException {
        System.out.println("Starting test - Thousand line construct SPARQL indexing");
        int limit = 1000;
        String indexName = "construct-thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Thousand line construct SPARQL indexing");
    }

    @Test
    public void fiveThousandConstructSPARQL() throws InterruptedException {
        System.out.println("Starting test - Five thousand line construct SPARQL indexing");
        int limit = 5000;
        String indexName = "construct-5thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Five thousand line construct SPARQL indexing");
    }

    @Test
    public void tenThousandConstructSPARQL() throws InterruptedException {
        System.out.println("Starting test - Ten thousand line construct SPARQL indexing");
        int limit = 10000;
        String indexName = "construct-10thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Ten thousand line construct SPARQL indexing");
    }
    @Test
    public void twentyThousandConstructSPARQL() throws InterruptedException {
        System.out.println("Starting test - Twenty thousand line construct SPARQL indexing");
        int limit = 20000;
        String indexName = "construct-20thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Twenty thousand line construct SPARQL indexing");
    }
    @Test
    public void fourtyThousandConstructSPARQL() throws InterruptedException {
        System.out.println("Starting test - Fourty thousand line construct SPARQL indexing");
        int limit = 40000;
        String indexName = "construct-40thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Fourty thousand line construct SPARQL indexing");
    }
}
