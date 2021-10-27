package org.elasticsearch.app.ApiSpringServer;

import org.elasticsearch.app.DAO.RiverDAO;
import org.elasticsearch.app.river.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Service
public class ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private final RiverDAO riverDAO;

    @Autowired
    public ConfigManager(RiverDAO riverDAO) {
        this.riverDAO = riverDAO;;
    }

    @Transactional
    public void save(River river) {
        riverDAO.save(river);
    }
    @Transactional
    public void delete(River river) {
        riverDAO.delete(river);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> getListOfConfigs() {
        Map<String, Object> configs = new HashMap<>();
        riverDAO.findAll().forEach((r)->configs.put(r.riverName(), r.toMap()));
        return configs;
    }

    @Transactional(readOnly = true)
    public River getRiver(String id) {
        return riverDAO.findById(id).orElse(null);
    }

}
