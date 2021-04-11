package org.elasticsearch.app.api.server.services;

import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.dao.RiverDAO;
import org.elasticsearch.app.api.server.entities.UpdateRecord;
import org.elasticsearch.app.api.server.scheduler.RunScheduledIndexing;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.api.server.entities.River;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

@Service
public class ConfigManager {

    private static final ESLogger logger = Loggers.getLogger(ConfigManager.class);

    private final RiverDAO riverDAO;

    private final ThreadPoolTaskScheduler taskScheduler;

    private final Indexer indexer;

    private final DashboardManager dashboardManager;

    private final Map<String, ScheduledFuture<?>> scheduledFutures = new HashMap<>();

    @Autowired
    public ConfigManager(RiverDAO riverDAO, ThreadPoolTaskScheduler taskScheduler, Indexer indexer, DashboardManager dashboardManager) {
        this.riverDAO = riverDAO;
        this.taskScheduler = taskScheduler;
        this.indexer = indexer;
        this.dashboardManager = dashboardManager;
        logger.setLevel(indexer.loglevel);
        createSchedule();
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
    public List<Map<String, Object>> getMapOfIndexes() {
        List<Map<String, Object>> indexes = new ArrayList<>();
        for (River river : riverDAO.findAll()) {
            Map<String, Object> riverMap = new HashMap<>();
            riverMap.put("name", river.getRiverName());
            riverMap.put("dashboards", getAssociatedDashboards(river.getRiverName()));
            riverMap.put("lastTenUpdateRecords", river.getLastTenUpdateRecords());
            indexes.add(riverMap);
        }
        return indexes;
    }

    @Transactional(readOnly = true)
    public River getRiver(String id) {
        return riverDAO.findById(id).orElse(null);
    }

    @Transactional
    public void addUpdateRecordToRiver(String indexName, UpdateRecord updateRecord) {
        River river = riverDAO.findById(indexName).orElse(null);
        if (Objects.isNull(river)) return;
        river.addUpdateRecord(updateRecord);
        riverDAO.save(river);
    }

    public Map<String, Object> getConfig(String id) {
        return getRiver(id).toMap();
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

    private Set<String> getAssociatedDashboards(String riverName) {
        Set<String> associatedDashboards = new HashSet<>();
        Map<String, Set<String>> associationOfIndexPatternsAndDashboards = dashboardManager.getAssociationOfIndexPatternsAndDashboards();
        for (String indexPatternRegex : associationOfIndexPatternsAndDashboards.keySet()) {
            if (riverName.matches("^" + indexPatternRegex.replace(".", "\\.").replace("*", ".*")))
                associatedDashboards.addAll(associationOfIndexPatternsAndDashboards.get(indexPatternRegex));
        }
        return associatedDashboards;
    }
}
