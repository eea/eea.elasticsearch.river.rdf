package org.elasticsearch.app.ApiSpringServer;

import org.elasticsearch.app.DAO.RiverDAO;
import org.elasticsearch.app.river.River;
import org.elasticsearch.common.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate5.SpringBeanContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.Schedules;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.servlet.view.tiles3.SpringBeanPreparerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@Service
public class ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private final RiverDAO riverDAO;

    private final ThreadPoolTaskScheduler taskScheduler;;

    @Autowired
    public ConfigManager(RiverDAO riverDAO, ThreadPoolTaskScheduler taskScheduler) {
        this.riverDAO = riverDAO;
        this.taskScheduler = taskScheduler;
        createSchedule();
    }

    @Transactional
    public void save(River river) {
        riverDAO.save(river);
        createSchedule();
    }

    @Transactional
    public void delete(River river) {
        riverDAO.delete(river);
        createSchedule();
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

    private void createSchedule() {;

        List<River> rivers = riverDAO.findAll();
        for (River river : rivers) {
            if (!river.isAutomatic()) continue;
            CronTrigger cronTrigger = new CronTrigger(river.getSchedule());
            ScheduledFuture<?> schedule = taskScheduler.schedule(new RunScheduledIndexing(river), cronTrigger);
            System.out.println("Scheduling " + river.riverName());
        }
        taskScheduler.setRemoveOnCancelPolicy(true);

    }

}
