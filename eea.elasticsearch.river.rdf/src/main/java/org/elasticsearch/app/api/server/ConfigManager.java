package org.elasticsearch.app.api.server;

import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.dao.RiverDAO;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.river.River;
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

    private final Map<String, ScheduledFuture<?>> scheduledFutures = new HashMap<>();

    @Autowired
    public ConfigManager(RiverDAO riverDAO, ThreadPoolTaskScheduler taskScheduler, Indexer indexer) {
        logger.setLevel(indexer.loglevel);
        this.riverDAO = riverDAO;
        this.taskScheduler = taskScheduler;
        this.indexer = indexer;
        createSchedule();
    }

    @Transactional
    public void save(River river) {
        addOrUpdateSchedule(river);
        riverDAO.save(river);
    }

    @Transactional
    public void delete(River river) {
        removeSchedule(river);
        riverDAO.delete(river);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> getListOfConfigs() {
        Map<String, Object> configs = new HashMap<>();
        riverDAO.findAll().forEach((r) -> configs.put(r.riverName(), r.toMap()));
        return configs;
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
        ScheduledFuture<?> scheduledFuture = scheduledFutures.get(river.riverName());
        if (Objects.isNull(scheduledFuture)) return;
        scheduledFuture.cancel(false);
        scheduledFutures.remove(scheduledFuture);
        logger.debug("Schedule for index '{}' - removed", river.riverName());
    }

    private void addOrUpdateSchedule(River river) {
        if (!river.isAutomatic()) {
            removeSchedule(river);
            return;
        }
        String state = "added";
        ScheduledFuture<?> scheduledFuture = scheduledFutures.get(river.riverName());
        if (Objects.nonNull(scheduledFuture)) {
            scheduledFuture.cancel(false);
            scheduledFutures.remove(scheduledFuture);
            state = "updated";
        }
        CronTrigger cronTrigger = new CronTrigger(river.getSchedule());
        scheduledFutures.put(river.riverName(), taskScheduler.schedule(new RunScheduledIndexing(river, indexer), cronTrigger));
        logger.debug("Schedule for index '{}' - {}", river.riverName(), state);
    }

}
