package org.elasticsearch.app.river;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class RiverSettings {
    private final Settings globalSettings;
    private final Map<String, Object> settings;

    public RiverSettings(Settings globalSettings, Map<String, Object> settings) {
        this.globalSettings = globalSettings;
        this.settings = settings;
    }

    public Settings globalSettings() {
        return this.globalSettings;
    }

    public Map<String, Object> settings() {
        return this.settings;
    }
}
