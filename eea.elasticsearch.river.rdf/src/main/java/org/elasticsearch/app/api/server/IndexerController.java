package org.elasticsearch.app.api.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.api.server.exceptions.AlreadyRunningException;
import org.elasticsearch.app.api.server.exceptions.ParsingException;
import org.elasticsearch.app.api.server.exceptions.SettingNotFoundException;
import org.elasticsearch.app.river.River;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


@RestController
@RequestMapping("/")
public class IndexerController {

    private final ConfigManager configManager;

    private final Indexer indexer;

    @Autowired
    public IndexerController(ConfigManager configManager, Indexer indexer) {
        this.configManager = configManager;
        this.indexer = indexer;
    }

    @GetMapping("/config")
    public Map<String, Object> getConfigs() {
        return configManager.getListOfConfigs();
    }

    @GetMapping("/running")
    public ArrayList<String> runningHarvests() {
        ArrayList<String> s = new ArrayList<>();
        for (RunningHarvester harvester : indexer.getHarvesterPool()) {
            s.add(harvester.getIndexName());
        }
        Collections.sort(s);
        return s;
    }

    @PutMapping(path = "/config", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String saveConfig(@RequestBody String s)  {
        Map<String, Object> map = null;
        try {
            map = new ObjectMapper().readValue(s, Map.class);
        } catch (JsonProcessingException e) {
            throw new ParsingException("Could not parse input JSON");
        }

        River river = new River(map);

        configManager.save(river);

        return river.riverName();
    }

    @PostMapping("/config/{id}/start")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void startIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            throw new SettingNotFoundException("Settings of index '" + id + "', not found");
        }
        if (runningHarvests().contains(river.riverName())) {
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
            throw new SettingNotFoundException("Settings of index '" + id + "', not found");
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
        throw new SettingNotFoundException("Indexing of index '" + id + "' is not running");
    }


}
