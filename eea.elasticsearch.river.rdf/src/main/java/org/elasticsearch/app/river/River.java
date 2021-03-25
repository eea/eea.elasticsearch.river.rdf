package org.elasticsearch.app.river;

import java.util.HashMap;
import java.util.Map;


public class River  {
    private String riverName;
    private Map<String,Object> riverSettings;

    public River() {
    }

    public River(Map<String, Object> map) {
        Map<String, Object> config = (Map<String, Object>) map.get("config");
        String name = ((Map) config.get("index")).get("index").toString();
        this.setRiverName(name).setRiverSettings(config);
    }

    public String riverName() {
        return this.riverName;
    }

    public River setRiverName(String riverName) {
        this.riverName =  riverName;
        return this;
    }

    public River setRiverSettings(Map<String,Object> settings) {
        this.riverSettings = settings;
        return this;
    }

    public Map<String,Object> getRiverSettings() {
        return this.riverSettings;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("config", getRiverSettings());

        return res;
    }
}
