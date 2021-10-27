package org.elasticsearch.app.river;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;

public class River {
    private RiverName riverName;
    private RiverSettings riverSettings;

    public River() {
    }

    public River(SearchHit sh) {

    }

    public River(Map<String, Object> map) {
        String name = ((Map) map.get("index")).get("index").toString();
        String type = ((Map) map.get("index")).get("type").toString();
        this.setRiverName(name, type).setRiverSettings(new RiverSettings(map));
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

    public String getJsonString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> res = new HashMap<>();

        res.put(riverName(), getRiverSettings().getSettings());

        return mapper.writeValueAsString(res);
    }
}
