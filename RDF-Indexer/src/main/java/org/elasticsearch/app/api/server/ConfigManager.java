package org.elasticsearch.app.api.server;

import javafx.util.Pair;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonString;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.dao.RiverDAO;
import org.elasticsearch.app.api.server.scheduler.RunScheduledIndexing;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.river.River;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class ConfigManager {

    private final int cacheDurationInSeconds = 10;

    private static final ESLogger logger = Loggers.getLogger(ConfigManager.class);

    private final RiverDAO riverDAO;

    private final ThreadPoolTaskScheduler taskScheduler;

    private final Indexer indexer;

    private final Map<String, ScheduledFuture<?>> scheduledFutures = new HashMap<>();

    private final CacheManager cacheManager;

    @Autowired
    public ConfigManager(RiverDAO riverDAO, ThreadPoolTaskScheduler taskScheduler, Indexer indexer, CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        logger.setLevel(indexer.loglevel);
        this.riverDAO = riverDAO;
        this.taskScheduler = taskScheduler;
        this.indexer = indexer;
        createSchedule();
        logger.setLevel(indexer.loglevel);
    }


    @Transactional
    public void save(River newRiver) {
        River foundRiver = riverDAO.findById(newRiver.getRiverName()).orElse(null);
        if (Objects.isNull(foundRiver)) foundRiver = newRiver;
        else foundRiver.update(newRiver);
        addOrUpdateSchedule(foundRiver);
        riverDAO.save(foundRiver);
    }

    @Transactional
    public void delete(River river) {
        removeSchedule(river);
        riverDAO.delete(river);
    }

    @Transactional(readOnly = true)
    @Cacheable("dashboardsInfo")
    public List<Map<String, Object>> getMapOfIndexes() {
        List<Map<String, Object>> indexes = new ArrayList<>();
        for (River river : riverDAO.findAll()) {
            Map<String, Object> riverMap = new HashMap<>();
            riverMap.put("name", river.getRiverName());
            riverMap.put("dashboards", getAssociatedDashboards(river.getRiverName()));
            riverMap.put("lastUpdate", river.getLastUpdate());
            riverMap.put("duration", river.getDuration());
            indexes.add(riverMap);
        }
        cacheEvictAfterTenSec();
        return indexes;
    }

    @Transactional(readOnly = true)
    public River getRiver(String id) {
        return riverDAO.findById(id).orElse(null);
    }

    private void createSchedule() {

        List<River> rivers = riverDAO.findAll();
        for (River river : rivers) {
            addOrUpdateSchedule(river);
        }

    }

    private void removeSchedule(River river) {
        ScheduledFuture<?> scheduledFuture = scheduledFutures.get(river.getRiverName());
        if (Objects.isNull(scheduledFuture)) return;
        scheduledFuture.cancel(false);
        scheduledFutures.remove(scheduledFuture);
        logger.debug("Schedule for index '{}' - removed", river.getRiverName());
    }

    private void addOrUpdateSchedule(River river) {
        if (!river.isAutomatic()) {
            removeSchedule(river);
            return;
        }
        String state = "added";
        ScheduledFuture<?> scheduledFuture = scheduledFutures.get(river.getRiverName());
        if (Objects.nonNull(scheduledFuture)) {
            scheduledFuture.cancel(false);
            scheduledFutures.remove(scheduledFuture);
            state = "updated";
        }
        CronTrigger cronTrigger = new CronTrigger(river.getSchedule());
        scheduledFutures.put(river.getRiverName(), taskScheduler.schedule(new RunScheduledIndexing(river, indexer), cronTrigger));
        logger.debug("Schedule for index '{}' - {}", river.getRiverName(), state);
    }


    private Map<String, Set<String>> getAssociationOfIndexPatternsAndDashboards() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.regexpQuery("type", "dashboard"));
        searchRequest.source(searchSourceBuilder);
        Map<String, Set<String>> mapIndexDashboards = new HashMap<>();
        try {
            SearchResponse searchResponse = indexer.clientES.search(searchRequest, RequestOptions.DEFAULT);
            for (Pair<String, String> dashboard : Arrays.stream(searchResponse.getHits().getHits()).map(hit -> new Pair<>(hit.getId(), ((Map) hit.getSourceAsMap().get("dashboard")).get("title").toString())).collect(Collectors.toList())) {
                Response kibanaResponse = indexer.clientKibana.getLowLevelClient().performRequest(new Request("GET", "/api/kibana/dashboards/export?" + dashboard.getKey().replace(':', '=')));

                JsonObject dashboardInfos = JSON.parse(kibanaResponse.getEntity().getContent());
                List<String> dashboardsIndexPatterns = dashboardInfos.get("objects").getAsArray().stream().filter(dashboardInfo ->
                        dashboardInfo.getAsObject().get("type").getAsString().equals(new JsonString("index-pattern"))
                ).map(pattern -> pattern.getAsObject().get("attributes").getAsObject().get("title").getAsString().value()).collect(Collectors.toList());
                for (String indexPatternRegex : dashboardsIndexPatterns) {
                    if (!mapIndexDashboards.containsKey(indexPatternRegex))
                        mapIndexDashboards.put(indexPatternRegex, new HashSet<>());
                    mapIndexDashboards.get(indexPatternRegex).add(dashboard.getValue());

                }
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(e.getMessage());
        }
        return mapIndexDashboards;
    }

    private Set<String> getAssociatedDashboards(String riverName) {
        Set<String> associatedDashboards = new HashSet<>();
        Map<String, Set<String>> associationOfIndexPatternsAndDashboards = getAssociationOfIndexPatternsAndDashboards();
        for (String indexPatternRegex : associationOfIndexPatternsAndDashboards.keySet()) {
            if (riverName.matches("^" + indexPatternRegex.replace(".", "\\.").replace("*", ".*")))
                associatedDashboards.addAll(associationOfIndexPatternsAndDashboards.get(indexPatternRegex));
        }
        return associatedDashboards;
    }

    public void cacheEvictAfterTenSec() {
        taskScheduler.getScheduledExecutor().schedule(() -> {
            Objects.requireNonNull(cacheManager.getCache("dashboardsInfo")).clear();
            logger.debug("Cache 'dashboardsInfo' cleared");
        }, cacheDurationInSeconds, TimeUnit.SECONDS);
    }

}
