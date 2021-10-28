package org.elasticsearch.app.api.server.services;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonString;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.exceptions.ConnectionLost;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.util.Pair;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class DashboardManager {

    private static final ESLogger logger = Loggers.getLogger(ConfigManager.class);

    private final Indexer indexer;

    private final CacheManager cacheManager;

    private final ThreadPoolTaskScheduler taskScheduler;

    @Autowired
    public DashboardManager(Indexer indexer, CacheManager cacheManager, ThreadPoolTaskScheduler taskScheduler) {
        this.indexer = indexer;
        this.cacheManager = cacheManager;
        this.taskScheduler = taskScheduler;
    }

    public void deleteIndex(String name) throws ConnectionLost {
        checkConnection();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(name);
        try {
            indexer.clientES.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException | ElasticsearchException e) {
            logger.error(e.getMessage());
        }
    }

    public String getKibanaHost() {
        return indexer.clientKibana.getLowLevelClient().getNodes().get(0).getHost().toString();
    }

    public void checkConnection() throws ConnectionLost {
        try {
            boolean ping = indexer.clientES.ping(RequestOptions.DEFAULT);
            Response response = indexer.clientKibana.getLowLevelClient().performRequest(new Request("GET", "/"));
            if (ping && Objects.nonNull(response))
                return;
        } catch (IOException | ElasticsearchException e) {
            logger.error(e.getMessage());
        }
        throw new ConnectionLost("Could not connect to ElasticSearch or Kibana");
    }

    @Cacheable("dashboardsInfo")
    public Map<String, Map<String, String>> getAssociationOfIndexPatternsAndDashboards() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.regexpQuery("type", "dashboard"));
        searchRequest.source(searchSourceBuilder);
        Map<String, Map<String, String>> mapIndexDashboards = new HashMap<>();
        try {
            SearchResponse searchResponse = indexer.clientES.search(searchRequest, RequestOptions.DEFAULT);
            for (Pair<String, String> dashboard : Arrays.stream(searchResponse.getHits().getHits()).map(hit -> Pair.of(hit.getId(), ((Map) hit.getSourceAsMap().get("dashboard")).get("title").toString())).collect(Collectors.toList())) {
                try {
                    Response kibanaResponse = indexer.clientKibana.getLowLevelClient().performRequest(new Request("GET", "/api/kibana/dashboards/export?" + dashboard.getFirst().replace(':', '=')));

                    JsonObject dashboardInfos = JSON.parse(kibanaResponse.getEntity().getContent());
                    List<String> dashboardsIndexPatterns;
                    dashboardsIndexPatterns = dashboardInfos.get("objects").getAsArray().stream().filter(dashboardInfo ->
                            dashboardInfo.getAsObject().get("type").getAsString().equals(new JsonString("index-pattern"))
                    ).map(pattern -> pattern.getAsObject().get("attributes").getAsObject().get("title").getAsString().value()).collect(Collectors.toList());
                    for (String indexPatternRegex : dashboardsIndexPatterns) {
                        if (!mapIndexDashboards.containsKey(indexPatternRegex))
                            mapIndexDashboards.put(indexPatternRegex, new HashMap<>());
                        mapIndexDashboards.get(indexPatternRegex).put(dashboard.getFirst().replace("dashboard:", ""), dashboard.getSecond());

                    }
                } catch (Exception e) {
                    logger.debug("No attributes in dashboard");
                }
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(e.getMessage());
        }
        cacheEvictAfter("dashboardsInfo", indexer.cacheDurationInSeconds);
        return mapIndexDashboards;
    }

    private void cacheEvictAfter(String cacheName, long cacheDurationInSeconds) {
        taskScheduler.getScheduledExecutor().schedule(() -> {
            Objects.requireNonNull(cacheManager.getCache(cacheName)).clear();
            logger.debug("Cache '{}' cleared", cacheName);
        }, cacheDurationInSeconds, TimeUnit.SECONDS);
    }
}
