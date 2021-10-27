package org.elasticsearch.app.api.server.dto;

import org.elasticsearch.app.api.server.entities.River;
import org.elasticsearch.app.api.server.entities.UpdateRecord;

import java.util.List;
import java.util.Map;

public class ConfigInfoDTO {
    private String name;
    private Map<String, String>  dashboards;
    private List<UpdateRecord> lastSuccessAndLastTenUpdateRecords;

    public ConfigInfoDTO(River river, Map<String, String>  dashboards) {
        this.name=river.getRiverName();
        this.dashboards=dashboards;
        this.lastSuccessAndLastTenUpdateRecords=river.getLastSuccessAndLastTenUpdateRecords();
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getDashboards() {
        return dashboards;
    }

    public List<UpdateRecord> getLastSuccessAndLastTenUpdateRecords() {
        return lastSuccessAndLastTenUpdateRecords;
    }
}
