package org.elasticsearch.app.river;

import org.elasticsearch.common.settings.Settings;
import java.util.Map;

public class RiverSettings {
    private final Settings globalSettings = null;
    private Map<String, Object> settings;

    public RiverSettings(Map<String, Object> source) {
        //this.globalSettings = globalSettings;
        this.settings = source;
    }

    public Settings globalSettings() {
        return this.globalSettings;
    }

    public RiverSettings settings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }

    public Map<String, Object> getSettings(){
        return this.settings;
    }
}