package  org.elasticsearch.app;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.river.River;
import org.elasticsearch.app.river.RiverName;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Indexer {
    private static final ESLogger logger = Loggers.getLogger(Harvester.class);

    private ArrayList<River> rivers = new ArrayList<>();

    private final static String USER = "user_rw";
    private final static String PASS = "rw_pass";
    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String RIVER_INDEX = "eeariver";

    private volatile Harvester harvester;

    private static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost(HOST, PORT, "http")
            ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            })
    );

    //private volatile Harvester harvester;
    //private volatile Thread harvesterThread;

    public static void main(String[] args) throws IOException {

        Indexer indexer = new Indexer();

        //InetAddress addr = InetAddress.getByName("127.0.0.1");

        /*RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            })
        );*/

        //MainResponse response = client.info();

        //TODO: for each river need to make a harvester
        /*Harvester harvester = new Harvester();

        //TODO: add river settings to harvester
        harvester.client(client).type("river").riverIndex("eeariver")
                //TODO: getFromSettingsFile
                .index("global-search");

        harvester.run();*/

        /*Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress(addr, 9200));*/


        //RDFRiver rdfRiver = new RDFRiver( new RiverName("river", "_river"), (RiverSettings) settings, "indexTemp", client );


        client.close();
        //harvester.run();


    }

    public Indexer() {
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(USER, PASS));

        getAllRivers();
    }

    private void getAllRivers() {
        ArrayList< SearchHit > searchHitsA = new ArrayList<>();

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(RIVER_INDEX);
        searchRequest.scroll(scroll);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest);

            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();

                //process the hits
                searchHitsA.addAll(Arrays.asList(searchHits));

            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
            boolean succeeded = clearScrollResponse.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (SearchHit  sh: searchHitsA ){
            Map<String, Object> source = sh.getSourceAsMap();
            Settings settings = getSettingsFromSource(source);

        }

    }

    private Settings getSettingsFromSource(Map<String, Object> source) {

    }


   /* protected void configure() {
        bind(River.class).to(RDFRiver.class).asEagerSingleton();
    }*/
}