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
public class SparqlSelectTest extends SPARQLTest {


    String endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
    String query = "PREFIX bd: <http://www.bigdata.com/rdf#> PREFIX cc: <http://creativecommons.org/ns#> PREFIX dct: <http://purl.org/dc/terms/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#> PREFIX owl: <http://www.w3.org/2002/07/owl#> PREFIX p: <http://www.wikidata.org/prop/> PREFIX pq: <http://www.wikidata.org/prop/qualifier/> PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/> PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/> PREFIX pr: <http://www.wikidata.org/prop/reference/> PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/> PREFIX prov: <http://www.w3.org/ns/prov#> PREFIX prv: <http://www.wikidata.org/prop/reference/value/> PREFIX ps: <http://www.wikidata.org/prop/statement/> PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/> PREFIX psv: <http://www.wikidata.org/prop/statement/value/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX schema: <http://schema.org/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/> PREFIX wdno: <http://www.wikidata.org/prop/novalue/> PREFIX wdref: <http://www.wikidata.org/reference/> PREFIX wds: <http://www.wikidata.org/entity/statement/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/> PREFIX wdv: <http://www.wikidata.org/value/> PREFIX wikibase: <http://wikiba.se/ontology#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT       distinct (?disease as ?s) ?p (?symptom_label as ?o) WHERE {       ?disease wdt:P780 ?symptom .        {       ?symptom rdfs:label ?symptom_label . FILTER(lang(?symptom_label) = \\\"en\\\")       SERVICE wikibase:label { bd:serviceParam wikibase:language \\\"en\\\". }       Bind( wdt:P780 as ?p)       } Union{         ?disease rdfs:label ?symptom_label .         FILTER (langMatches( lang(?symptom_label), \\\"EN\\\" ) )         Bind(  rdfs:label as ?p)       } }";
    String queryType = "select";

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Test
    public void thousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Thousand line select SPARQL indexing");
        int limit = 1000;
        String indexName = "select-thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Thousand line select SPARQL indexing");
    }

    @Test
    public void fiveThousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Five thousand line select SPARQL indexing");
        int limit = 5000;
        String indexName = "select-5thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Five thousand line select SPARQL indexing");
    }

    @Test
    public void tenThousandSelectSPARQL() throws InterruptedException {
        System.out.println("Starting test - Ten thousand line select SPARQL indexing");
        int limit = 10000;
        String indexName = "select-10thousand";
        executeSPARQLQuery(query, endpoint, queryType, limit, indexName);
        System.out.println("\nTest successful - Ten thousand line select SPARQL indexing");
    }
}
