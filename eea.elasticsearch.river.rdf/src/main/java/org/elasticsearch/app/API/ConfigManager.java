package org.elasticsearch.app.API;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.river.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
public class ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private Map<String, Object> configs;

    private final ObjectMapper mapper = new ObjectMapper();

    private boolean dirty = false;

    private final File file = new File("indexingConfigs.json");

    public ConfigManager() {
        load();
    }

    public void add(River river) {
        configs.put(river.riverName(), river.toMap());
        dirty = true;
        save();
    }

    public void remove(River river) {
        configs.remove(river.riverName());
        dirty = true;
        save();
    }

    public Map<String, Object> getListOfConfigs() {
        return configs;
    }

    public River getRiver(String id) {
        if (!configs.containsKey(id)) return null;
        return new River((Map<String, Object>) configs.get(id));
    }

    public void save() {
        //TODO: make saving on timer
        if (!dirty) return;
        dirty = false;
        BufferedWriter writer = null;
        try {
            String configJson = mapper.writeValueAsString(configs);
            writer = new BufferedWriter(new FileWriter(file, false));
            writer.write(configJson);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            if (writer != null) writer.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void load() {
        dirty = false;
        if (!file.exists()) {
            configs = new HashMap<>();
            return;
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String configJson = reader.readLine();
            configs = mapper.readValue(configJson, Map.class);
            reader.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
