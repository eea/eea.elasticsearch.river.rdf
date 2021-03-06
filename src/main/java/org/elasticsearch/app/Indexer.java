package  org.elasticsearch.app;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.xpath.internal.operations.Bool;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.river.River;
import org.elasticsearch.app.river.RiverName;
import org.elasticsearch.app.river.RiverSettings;
import org.elasticsearch.client.*;

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Indexer {
    private final static String USER = "user_rw";
    private final static String PASS = "rw_pass";
    private final static String HOST = "localhost";
    private final static int PORT = 9200;

    private String RIVER_INDEX = "eeariver";

    public String getRIVER_INDEX() {
        return RIVER_INDEX;
    }

    private boolean MULTITHREADING_ACTIVE = false;
    private int THREADS = 1;
    public String loglevel;

    private static final ESLogger logger = Loggers.getLogger(Indexer.class);

    private ArrayList<River> rivers = new ArrayList<>();

    public Map<String, String> envMap;

    private static final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    public RestHighLevelClient client;

    private static ExecutorService executorService;

    public static void main(String[] args) throws IOException {

        logger.info("Starting application...");

        Indexer indexer = new Indexer();
        logger.setLevel(indexer.loglevel);

        if(indexer.rivers.size() == 0){
            logger.info("No rivers detected");
            logger.info("No rivers added in " + indexer.RIVER_INDEX + " index.Stopping...");
            indexer.close();
        }

        //TODO: loop for all rivers
        if(indexer.MULTITHREADING_ACTIVE){
        /*Indexer.executorService = EsExecutors.newAutoQueueFixed("threadPool", 1, 5, 5, 26,2,
                TimeValue.timeValueHours(10), EsExecutors.daemonThreadFactory("esapp"), new ThreadContext(Builder.EMPTY_SETTINGS));*/
            Indexer.executorService = Executors.newFixedThreadPool(indexer.THREADS);
            //Indexer.executorService = Executors.newWorkStealingPool(4);

        } else {
            Indexer.executorService = Executors.newSingleThreadExecutor();
        }

        for(River river : indexer.rivers){
            Harvester h = new Harvester();

            h.client(indexer.client).riverName(river.riverName())
                    .riverIndex(indexer.RIVER_INDEX)
                    .indexer(indexer);
            indexer.addHarvesterSettings(h, river.getRiverSettings());

            Indexer.executorService.submit(h);
            logger.info("Created thread for river: {}", river.riverName());
        }

        Indexer.executorService.shutdown();

        logger.info("All tasks submitted.");
        try {
            Indexer.executorService.awaitTermination(1, TimeUnit.DAYS);

            try {
                DeleteIndexRequest request = new DeleteIndexRequest(indexer.RIVER_INDEX);
                indexer.client.indices().delete(request);
                logger.info("Deleting river index!!!");

            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    logger.error("River index not found");
                    logger.info("Tasks interrupted by missing river index.");
                    indexer.close();
                }
            }

        } catch (InterruptedException ignored) {
            logger.info("Tasks interrupted.");
        }
        logger.info("All tasks completed.");

        // Switching alias
        if(indexer.rivers.size() > 0){
            River riv = indexer.rivers.get(0);

            HashMap set = (HashMap) riv.getRiverSettings().getSettings().get("syncReq");
            HashMap ind = (HashMap) set.get("index");

            if(ind != null){
                Boolean switchA = (Boolean) ind.get("switchAlias");

                if(switchA != null && switchA){
                    RestClient lowclient = indexer.client.getLowLevelClient();

                    switchAliases(lowclient, indexer);
                }

            }
        }
        indexer.close();

    }

    private static void switchAliases(RestClient lowclient, Indexer indexer) {
        Response response = null;
        String indexA = "";

        try {
            response = lowclient.performRequest("GET", "global-search/_mappings");
            String responseBody = EntityUtils.toString(response.getEntity());

            HashMap myMap = new HashMap<String, String>();

            ObjectMapper objectMapper = new ObjectMapper();
            String reali = "";

            try {
                myMap = objectMapper.readValue(responseBody, HashMap.class);
                reali = (String) myMap.keySet().toArray()[0].toString();

                String alias = reali.replace("_green", "").replace("_blue", "");
                // switching aliases
                if(reali.contains("_green")){
                    indexA = reali.replace("_green", "_blue");
                } else if(reali.contains("_blue")){
                    indexA = reali.replace("_blue", "_green");
                }

                Map<String, String> params = Collections.emptyMap();

                String jsonStringRemove = "{ " +
                        "\"actions\" : [ " +
                            "{ \"remove\" : {" +
                                "\"index\" : \""+ reali +"\"," +
                                "\"alias\" : \""+ alias +"\"" +
                                "}" +
                            " }" + "," +
                            "{ \"remove\" : {" +
                                "\"index\" : \""+ reali + "_status" +"\"," +
                                "\"alias\" : \""+ alias + "_status" +"\"" +
                                "}" +
                            " }" +
                        "]}";

                HttpEntity entityR = new NStringEntity(jsonStringRemove, ContentType.APPLICATION_JSON);
                Response responseRemove = lowclient.performRequest("POST", "/_aliases", params, entityR);

                if(responseRemove.getStatusLine().getStatusCode() == 200){
                    logger.info("{}", EntityUtils.toString(responseRemove.getEntity()) );
                    logger.info("Removed alias from index: " + reali);
                }

                String jsonStringAdd = "{ " +
                        "\"actions\" : [ " +
                            "{ \"add\" : {" +
                                "\"index\" : \""+ indexA +"\"," +
                                "\"alias\" : \""+ alias +"\"" +
                                "}" +
                            " }" + "," +
                        "{ \"add\" : {" +
                                "\"index\" : \""+ indexA + "_status" +"\"," +
                                "\"alias\" : \""+ alias + "_status" +"\"" +
                                "}" +
                            " }" +
                        "]}";

                HttpEntity entityAdd = new NStringEntity(jsonStringAdd, ContentType.APPLICATION_JSON);
                try {
                    Response responseAdd = lowclient.performRequest("POST", "/_aliases", params, entityAdd);

                    if(responseAdd.getStatusLine().getStatusCode() == 200){
                        logger.info("{}", EntityUtils.toString(responseAdd.getEntity()) );
                        logger.info("Added alias to index: " + indexA);
                    }

                } catch (IOException exe){
                    logger.error("Could not add alias to index : {}; reason: {}", indexA, exe.getMessage());
                }


            } catch (IOException ex){
                logger.error("Could not remove alias from index : {}; reason: {}", reali, ex.getMessage());
            }

        } catch (IOException e){
            logger.error("Could not get aliases from index; reason: {}", e.getMessage());
        }
    }

    public Indexer() {
        Map<String, String> env = System.getenv();
        this.envMap = env;

        String host = (env.get("elastic_host") != null) ? env.get("elastic_host") : HOST;

        int port = (env.get("elastic_port") != null) ? Integer.parseInt(env.get("elastic_port")) : PORT;
        String user = (env.get("elastic_user") != null) ? env.get("elastic_user") : USER;
        String pass = (env.get("elastic_pass") != null) ? env.get("elastic_pass") : PASS;
        this.RIVER_INDEX = ( env.get("river_index") != null) ? env.get("river_index") : this.RIVER_INDEX;
        this.MULTITHREADING_ACTIVE  = (env.get("indexer_multithreading") != null) ?
                Boolean.parseBoolean(env.get("indexer_multithreading")) : this.MULTITHREADING_ACTIVE;
        this.THREADS = (env.get("threads") != null) ? Integer.parseInt(env.get("threads")) : this.THREADS;
        this.loglevel = ( env.get("LOG_LEVEL") != null) ? env.get("LOG_LEVEL") : "info";

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user , pass));

        client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost( host , port, "http")
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

        logger.debug("Username: " + this.envMap.get("elastic_user"));
        logger.debug("Password: " + this.envMap.get("elastic_pass"));
        logger.debug("HOST: " + this.envMap.get("elastic_host"));
        logger.debug("PORT: " + this.envMap.get("elastic_port"));
        logger.debug("RIVER INDEX: " + this.RIVER_INDEX);
        logger.debug("MULTITHREADING_ACTIVE: " + this.MULTITHREADING_ACTIVE);
        logger.debug("THREADS: " + this.THREADS);
        logger.info("LOG_LEVEL: " + this.envMap.get("LOG_LEVEL") );
        logger.debug("DOCUMENT BULK: ", Integer.toString(EEASettings.DEFAULT_BULK_REQ) );
        getAllRivers();
    }

    public void getRivers(){
        this.getAllRivers();

    }

    private void getAllRivers() {
        this.rivers.clear();
        ArrayList< SearchHit > searchHitsA = new ArrayList<>();

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(this.RIVER_INDEX);
        searchRequest.scroll(scroll);

        SearchResponse searchResponse = null;
        try {
            logger.info("{}",searchRequest);
            searchResponse = client.search(searchRequest);
            logger.info("River index {} found", this.RIVER_INDEX);
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
            logger.info("River index " + this.RIVER_INDEX + " not found");
            System.exit(0);
        } catch (ElasticsearchStatusException ex){
            logger.info(ex.toString());
            logger.info("River index " + this.RIVER_INDEX + " not found");
            System.exit(0);
        }

        for (SearchHit  sh: searchHitsA ){
            Map<String, Object> source = sh.getSourceAsMap();
            //logger.debug("{}", source.containsKey("eeaRDF"));

            if(source.containsKey("eeaRDF")){
                RiverSettings riverSettings = new RiverSettings(source);
                RiverName riverName = new RiverName("eeaRDF", sh.getId());
                River river = new River()
                        .setRiverName( riverName.name() )
                        .setRiverSettings( riverSettings );
                rivers.add(river);
                continue;
            }

            if ( !((Map)source.get("syncReq")).containsKey("eeaRDF")) {
                logger.error( "not has river settings: " + sh.getId() );
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

    private void addHarvesterSettings(Harvester harv, RiverSettings settings) {
        if(settings.getSettings().containsKey("eeaRDF")){

        } else if ( ! (((HashMap)settings.getSettings().get("syncReq")).containsKey("eeaRDF")) ) {
            logger.error("There are no syncReq settings in the river settings");
            throw new IllegalArgumentException(
                    "There are no eeaRDF settings in the river settings");
        }

        Map<String, Object> rdfSettings = extractSettings(settings, "eeaRDF");

        harv.rdfIndexType(XContentMapValues.nodeStringValue(
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
            harv.rdfPropList(getStrListFromSettings(rdfSettings, "proplist"));
        }
        if(rdfSettings.containsKey("query")) {
            harv.rdfQuery(getStrListFromSettings(rdfSettings, "query"));
        } else {
            harv.rdfQuery(EEASettings.DEFAULT_QUERIES);
        }

        if(rdfSettings.containsKey("normProp")) {
            harv.rdfNormalizationProp(getStrObjMapFromSettings(rdfSettings, "normProp"));
        }
        if(rdfSettings.containsKey("normMissing")) {
            harv.rdfNormalizationMissing(getStrObjMapFromSettings(rdfSettings, "normMissing"));
        }
        if(rdfSettings.containsKey("normObj")) {
            harv.rdfNormalizationObj(getStrStrMapFromSettings(rdfSettings, "normObj"));
        }
        if(rdfSettings.containsKey("blackMap")) {
            harv.rdfBlackMap(getStrObjMapFromSettings(rdfSettings, "blackMap"));
        }
        if(rdfSettings.containsKey("whiteMap")) {
            harv.rdfWhiteMap(getStrObjMapFromSettings(rdfSettings, "whiteMap"));
        }
        //TODO : change to index
        if(settings.getSettings().containsKey("index")){
            Map<String, Object> indexSettings = extractSettings(settings, "index");
            harv.index(XContentMapValues.nodeStringValue(
                    indexSettings.get("index"),
                    EEASettings.DEFAULT_INDEX_NAME)
            )
                    .type(XContentMapValues.nodeStringValue(
                            indexSettings.get("type"),
                            EEASettings.DEFAULT_TYPE_NAME)

            )
                    .statusIndex(XContentMapValues.nodeStringValue(indexSettings.get("statusIndex"),EEASettings.DEFAULT_INDEX_NAME + "_status")
            );
        }
        else {
            //TODO: don't know if is correct
            if( settings.getSettings().containsKey("syncReq")){
                harv.index(  ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index")).get("index").toString() );
                harv.type( ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index")).get("type").toString() );

                String indexName = ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index")).get("index").toString();

                HashMap indexMap = ((HashMap)((HashMap)settings.getSettings().get("syncReq")).get("index"));
                String statusI = indexMap.get("statusIndex") != null ? indexMap.get("statusIndex").toString() : indexName + "_status";
                harv.statusIndex( statusI );
            } else {
                harv.index(EEASettings.DEFAULT_INDEX_NAME);
                harv.type( "river" );
                harv.statusIndex(EEASettings.DEFAULT_INDEX_NAME + "_status");
            }

        }

    }

    /** Type casting accessors for river settings **/
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractSettings(RiverSettings settings,
                                                       String key) {
        if(settings.getSettings().containsKey("eeaRDF")){
            return (Map<String, Object>) ( (Map<String, Object>)settings.getSettings()).get(key);
        } else {
            return (Map<String, Object>) ( (Map<String, Object>)settings.getSettings().get("syncReq")).get(key);
        }

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

    }

    public void close() {
        System.exit(0);
    }

    public void closeHarvester(Harvester that) {
        logger.info("Closing thread");
    }

}