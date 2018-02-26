package  org.elasticsearch.app;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.action.search.*;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.river.River;
import org.elasticsearch.app.river.RiverName;
import org.elasticsearch.app.river.RiverSettings;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class Indexer {
    private static final ESLogger logger = Loggers.getLogger(Indexer.class);
    private ArrayList<River> rivers = new ArrayList<>();
    private final static String USER = "user_rw";
    private final static String PASS = "rw_pass";
    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String RIVER_INDEX = "eeariver";

    private final static boolean MULTITHREADING_ACTIVE = false;

    private volatile Harvester harvester;
    private volatile Thread harvesterThread;

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
        .setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(HttpHost host) {
                super.onFailure(host);
                logger.error("Connection failure: [{}]", host);
            }
        })
    );

    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer();

        if( MULTITHREADING_ACTIVE ){
            for(River river : indexer.rivers){
                indexer.harvester = new Harvester();
                indexer.harvester.client(client).riverName( river.riverName())
                    .riverIndex(RIVER_INDEX);
                indexer.addHarvesterSettings(river.getRiverSettings());
                indexer.start();
            }
        }

        logger.info("Username:" + USER);
        logger.info("Password: " + PASS);
        logger.info("HOST: " + HOST);
        logger.info("PORT: " + PORT );
        logger.info("RIVER INDEX: " + RIVER_INDEX);
        
        River river = indexer.rivers.get(0);

        indexer.harvester = new Harvester();
        indexer.harvester.client(client).riverName( river.riverName() ).riverIndex(RIVER_INDEX);
        indexer.addHarvesterSettings(river.getRiverSettings());
        indexer.start();

        //InetAddress addr = InetAddress.getByName("127.0.0.1");

        /*Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress(addr, 9200));*/

        //client.close();
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

            //process the hits
            searchHitsA.addAll(Arrays.asList(searchHits));

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
            logger.info("River index not found");
        }

        for (SearchHit  sh: searchHitsA ){
            Map<String, Object> source = sh.getSourceAsMap();

            if (!((Map)source.get("syncReq")).containsKey("eeaRDF")) {
                System.out.println( "not has river settings: " + sh.getId() );
                throw new IllegalArgumentException(
                        "There are no eeaRDF settings in the river settings");
            } else {
                RiverSettings riverSettings = new RiverSettings(source);
                RiverName riverName = new RiverName("eeaRDF", sh.getId());
                River river = new River()
                        .setRiverName( riverName.name() )
                        .setRiverSettings( riverSettings );
                rivers.add(river);
            }
        }
    }

    private void addHarvesterSettings(RiverSettings settings) {
        if (!((HashMap)settings.getSettings().get("syncReq")).containsKey("eeaRDF")) {
            throw new IllegalArgumentException(
                    "There are no eeaRDF settings in the river settings");
        }

        Map<String, Object> rdfSettings = extractSettings(settings, "eeaRDF");



        harvester.rdfIndexType(XContentMapValues.nodeStringValue(
                rdfSettings.get("indexType"), "full"))
                .rdfStartTime(XContentMapValues.nodeStringValue(
                        rdfSettings.get("startTime"),""))
                .rdfUris(XContentMapValues.nodeStringValue(
                        rdfSettings.get("uris"), "[]"))
                .rdfEndpoint(XContentMapValues.nodeStringValue(
                        rdfSettings.get("endpoint"),
                        EEASettings.DEFAULT_ENDPOINT))
                .rdfClusterId(XContentMapValues.nodeStringValue(
                        rdfSettings.get("cluster_id"),
                        EEASettings.DEFAULT_CLUSTER_ID))
                .rdfQueryType(XContentMapValues.nodeStringValue(
                        rdfSettings.get("queryType"),
                        EEASettings.DEFAULT_QUERYTYPE))
                .rdfListType(XContentMapValues.nodeStringValue(
                        rdfSettings.get("listtype"),
                        EEASettings.DEFAULT_LIST_TYPE))
                .rdfAddLanguage(XContentMapValues.nodeBooleanValue(
                        rdfSettings.get("addLanguage"),
                        EEASettings.DEFAULT_ADD_LANGUAGE))
                .rdfAddCounting(XContentMapValues.nodeBooleanValue(
                        rdfSettings.get("addCounting"),
                        EEASettings.DEFAULT_ADD_COUNTING))
                .rdfLanguage(XContentMapValues.nodeStringValue(
                        rdfSettings.get("language"),
                        EEASettings.DEFAULT_LANGUAGE))
                .rdfAddUriForResource(XContentMapValues.nodeBooleanValue(
                        rdfSettings.get("includeResourceURI"),
                        EEASettings.DEFAULT_ADD_URI))
                .rdfURIDescription(XContentMapValues.nodeStringValue(
                        rdfSettings.get("uriDescription"),
                        EEASettings.DEFAULT_URI_DESCRIPTION))
                .rdfSyncConditions(XContentMapValues.nodeStringValue(
                        rdfSettings.get("syncConditions"),
                        EEASettings.DEFAULT_SYNC_COND))
                .rdfGraphSyncConditions(XContentMapValues.nodeStringValue(
                        rdfSettings.get("graphSyncConditions"), ""))
                .rdfSyncTimeProp(XContentMapValues.nodeStringValue(
                        rdfSettings.get("syncTimeProp"),
                        EEASettings.DEFAULT_SYNC_TIME_PROP))
                .rdfSyncOldData(XContentMapValues.nodeBooleanValue(
                        rdfSettings.get("syncOldData"),
                        EEASettings.DEFAULT_SYNC_OLD_DATA));

        if (rdfSettings.containsKey("proplist")) {
            harvester.rdfPropList(getStrListFromSettings(rdfSettings, "proplist"));
        }
        if(rdfSettings.containsKey("query")) {
            harvester.rdfQuery(getStrListFromSettings(rdfSettings, "query"));
        } else {
            harvester.rdfQuery(EEASettings.DEFAULT_QUERIES);
        }

        if(rdfSettings.containsKey("normProp")) {
            harvester.rdfNormalizationProp(getStrObjMapFromSettings(rdfSettings, "normProp"));
        }
        if(rdfSettings.containsKey("normMissing")) {
            harvester.rdfNormalizationMissing(getStrObjMapFromSettings(rdfSettings, "normMissing"));
        }
        if(rdfSettings.containsKey("normObj")) {
            harvester.rdfNormalizationObj(getStrStrMapFromSettings(rdfSettings, "normObj"));
        }
        if(rdfSettings.containsKey("blackMap")) {
            harvester.rdfBlackMap(getStrObjMapFromSettings(rdfSettings, "blackMap"));
        }
        if(rdfSettings.containsKey("whiteMap")) {
            harvester.rdfWhiteMap(getStrObjMapFromSettings(rdfSettings, "whiteMap"));
        }
        //TODO : change to index
        if(settings.getSettings().containsKey("index")){
            Map<String, Object> indexSettings = extractSettings(settings, "index");
            harvester.index(XContentMapValues.nodeStringValue(
                    indexSettings.get("index"),
                    EEASettings.DEFAULT_INDEX_NAME))
                    .type(XContentMapValues.nodeStringValue(
                            indexSettings.get("type"),
                            EEASettings.DEFAULT_TYPE_NAME));
        }
        else {
            //TODO: don't know if is correct
            harvester.index(  ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index")).get("index").toString() );
            harvester.type( ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index")).get("type").toString() );

            //harvester.index(EEASettings.DEFAULT_INDEX_NAME).type(EEASettings.DEFAULT_TYPE_NAME);
        }

    }

    /** Type casting accessors for river settings **/
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractSettings(RiverSettings settings,
                                                       String key) {
        return (Map<String, Object>) ( (Map<String, Object>)settings.getSettings().get("syncReq")).get(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getStrStrMapFromSettings(Map<String, Object> settings,
                                                                String key) {
        return (Map<String, String>)settings.get(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getStrObjMapFromSettings(Map<String, Object> settings,
                                                                String key) {
        return (Map<String, Object>)settings.get(key);
    }

    @SuppressWarnings("unchecked")
    private static List<String> getStrListFromSettings(Map<String, Object> settings,
                                                       String key) {
        return (List<String>)settings.get(key);
    }

    public void start() {
        harvesterThread = EsExecutors.daemonThreadFactory(
                Builder.EMPTY_SETTINGS,
                "eea_rdf_river(" + harvester.getRiverName() +	")")
                .newThread(harvester);

        harvesterThread.start();
        logger.info( "Inside thread : " + harvesterThread.getName());
        harvesterThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Thread FAILED: [{}] " , (Object) e.getStackTrace());

            }
        });
    }

    public void close() {
        harvester.log("Closing EEA RDF river [" + harvester.getRiverName() + "]");
        harvester.close();
        harvesterThread.interrupt();
    }

   /* protected void configure() {
        bind(River.class).to(RDFRiver.class).asEagerSingleton();
    }*/
}