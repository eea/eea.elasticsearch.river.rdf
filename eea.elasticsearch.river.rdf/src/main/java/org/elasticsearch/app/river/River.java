package org.elasticsearch.app.river;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class River implements Serializable {
    private RiverName riverName;
    private RiverSettings riverSettings;

    public River() {
    }

    public River(Map<String, Object> map) {
        Map<String, Object> config = (Map<String, Object>) map.get("config");
        String name = ((Map) config.get("index")).get("index").toString();
        String type = ((Map) config.get("index")).get("type").toString();
        this.setRiverName(name, type).setRiverSettings(new RiverSettings(config));
    }

    public String riverName() {
        return this.riverName.name();
    }

    public River setRiverName(String riverName) {
        this.riverName = new RiverName("", riverName);
        return this;
    }

    public River setRiverName(String riverName, String type) {
        this.riverName = new RiverName("", riverName);
        return this;
    }

    public River setRiverSettings(RiverSettings settings) {
        this.riverSettings = settings;
        return this;
    }

    public RiverSettings getRiverSettings() {
        return this.riverSettings;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("config", getRiverSettings().getSettings());

        return res;
    }
}
