package org.elasticsearch.app.api.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.exceptions.AlreadyRunningException;
import org.elasticsearch.app.api.server.exceptions.ParsingException;
import org.elasticsearch.app.api.server.exceptions.ConfigNotFoundException;
import org.elasticsearch.app.api.server.scheduler.RunningHarvester;
import org.elasticsearch.app.api.server.entities.River;
import org.elasticsearch.app.api.server.services.ConfigManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RestController
@CrossOrigin("*")
@RequestMapping("/")
public class IndexerController {

    private final ConfigManager configManager;

    private final Indexer indexer;

    @Autowired
    public IndexerController(ConfigManager configManager, Indexer indexer) {
        this.configManager = configManager;
        this.indexer = indexer;
        this.indexer.configManager = this.configManager;
    }

    @GetMapping("/configs")
    public List<Map<String, Object>> getConfigs() {
        return configManager.getMapOfIndexes();
    }

    @GetMapping("/running")
    public Map<String, String> runningHarvests() {
        Map<String, String> running = new HashMap<>();
        for (RunningHarvester harvester : indexer.getHarvesterPool()) {
            running.put(harvester.getIndexName(), harvester.getHarvestState().toString());
        }
        return running;
    }

    @GetMapping("/config/{id}")
    public Map<String, Object> getConfigs(@PathVariable String id) {
        return configManager.getConfig(id);
    }

    @PutMapping(path = "/config", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String saveConfig(@RequestBody String s) {
        Map<String, Object> map;
        try {
            map = new ObjectMapper().readValue(s, Map.class);
        } catch (JsonProcessingException e) {
            throw new ParsingException("Could not parse input JSON");
        }

        River river = new River(map);

        configManager.save(river);

        return river.getRiverName();
    }

    @PostMapping("/config/{id}/start")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void startIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            throw new ConfigNotFoundException("Settings of index '" + id + "', not found");
        }
        if (indexer.getHarvesterPool().stream().anyMatch(h -> h.getIndexName().equals(river.getRiverName()))) {
            throw new AlreadyRunningException("Indexing of index '" + id + "', already running");
        }

        indexer.setRivers(river);
        indexer.startIndexing();


    }

    @PostMapping(path = "/configAndIndex", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String saveConfigAndStart(@RequestBody String s) {
        String id = saveConfig(s);
        startIndex(id);
        return id;
    }

    @DeleteMapping("/config/{id}")
    public void deleteIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            throw new ConfigNotFoundException("Settings of index '" + id + "', not found");
        }
        configManager.delete(river);
    }

    @PostMapping("/config/{id}/stop")
    public void stopIndex(@PathVariable String id) {
        for (RunningHarvester harvester : indexer.getHarvesterPool()) {
            if (harvester.getIndexName().equals(id)) {
                harvester.stop();
                return;
            }
        }
        throw new ConfigNotFoundException("Indexing of index '" + id + "' is not running");
    }


}
