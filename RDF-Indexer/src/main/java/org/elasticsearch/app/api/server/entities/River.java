package org.elasticsearch.app.api.server.entities;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import java.util.*;
import java.util.stream.Collectors;

@Entity
public class River {

    @Transient
    private static final Logger logger = LoggerFactory.getLogger(River.class);

    @Id
    private String riverName;

    @Lob
    @Column(columnDefinition = "CLOB")
    private String settingsJSON;

    @Column
    @Basic
    private String schedule;

    @Column
    @Basic
    private boolean automaticScheduling;

    @OneToMany(cascade = CascadeType.ALL)
    private List<UpdateRecord> updateHistory = new ArrayList<>();

    @Transient
    private Map<String, Object> riverSettings;

    public River() {
    }

    public River(Map<String, Object> map) {
        Map<String, Object> config = (Map<String, Object>) map.get("config");
        Map<String, Object> scheduleMap = (Map<String, Object>) map.get("schedule");
        String name = ((Map) config.get("index")).get("index").toString();
        automaticScheduling = (boolean) scheduleMap.get("automatic");
        this.setSchedule(scheduleMap.get("schedule").toString());
        this.setRiverName(name);
        this.setRiverSettings(config);
    }

    public boolean isAutomatic() {
        return automaticScheduling;
    }

    public String getSchedule() {
        return schedule;
    }


    public void setSchedule(String schedule) {
        //TODO: parse for cron
        this.schedule = schedule;
    }

    public String getRiverName() {
        return this.riverName;
    }

    public River setRiverName(String riverName) {
        this.riverName = riverName;
        return this;
    }

    public River setRiverSettings(Map<String, Object> settings) {
        try {
            settingsJSON = new ObjectMapper().writeValueAsString(settings);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
        this.riverSettings = settings;
        return this;
    }

    public Map<String, Object> getRiverSettings() {
        if (riverSettings == null) try {
            riverSettings = new ObjectMapper().readValue(settingsJSON, Map.class);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
        return riverSettings;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> res = new HashMap<>();
        Map<String, Object> scheduleMap = new HashMap<>();
        scheduleMap.put("schedule", schedule);
        scheduleMap.put("automatic", automaticScheduling);
        res.put("schedule", scheduleMap);
        res.put("config", getRiverSettings());
        return res;
    }

    public void update(River newRiver) {
        setRiverSettings(newRiver.getRiverSettings());
        automaticScheduling = newRiver.isAutomatic();
        setSchedule(newRiver.getSchedule());
    }

    public List<UpdateRecord> getLastTenUpdateRecords() {
        return updateHistory.stream().sorted(Comparator.comparing(UpdateRecord::getLastUpdateStartDate,Comparator.nullsLast(Comparator.reverseOrder()))).limit(10).collect(Collectors.toList());
    }

    public void addUpdateRecord(UpdateRecord updateRecord) {
        updateHistory.add(updateRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.riverName, this.settingsJSON);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof River))
            return false;
        River river = (River) obj;
        return Objects.equals(this.riverName, river.riverName) && Objects.equals(this.riverSettings, river.riverSettings);
    }

    @Override
    public String toString() {
        return "River: " + this.riverName + " = " + this.toMap().toString();

    }
}
