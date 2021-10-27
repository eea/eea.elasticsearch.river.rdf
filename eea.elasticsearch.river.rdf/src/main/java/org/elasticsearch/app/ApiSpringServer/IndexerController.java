package org.elasticsearch.app.ApiSpringServer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.ApiServer;
import org.elasticsearch.app.ApiSpringServer.Exceptions.AlreadyRunningException;
import org.elasticsearch.app.ApiSpringServer.Exceptions.ParsingException;
import org.elasticsearch.app.ApiSpringServer.Exceptions.SettingNotFoundException;
import org.elasticsearch.app.river.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(IndexerController.class);

    private final ConfigManager configManager;

    @Autowired
    public IndexerController(ConfigManager configManager) {
        this.configManager = configManager;
    }

    @GetMapping("/config")
    public Map<String, Object> SaveConfig() {
        return configManager.getListOfConfigs();
    }

    @GetMapping("/running")
    public ArrayList<String> runningHarvests() {
        ArrayList<String> s = new ArrayList<>();
        for (RunningHarvester harvester : ApiServer.indexer.getHarvesterPool()) {
            s.add(harvester.getIndexName());
        }
        Collections.sort(s);
        return s;
    }

    @PutMapping(path = "/config", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfig(@RequestBody String s)  {
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
    public void StartIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            throw new SettingNotFoundException("Settings of index '" + id + "', not found");
        }
        if (runningHarvests().contains(river.riverName())) {
            throw new AlreadyRunningException("Indexing of index '" + id + "', already running");
        }

        ApiServer.indexer.setRivers(river);
        ApiServer.indexer.startIndexing();


    }

    @PostMapping(path = "/configAndIndex", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfigAndStart(@RequestBody String s) {
        String id = SaveConfig(s);
        StartIndex(id);
        return id;
    }

    @DeleteMapping("/config/{id}")
    public void DeleteIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            throw new SettingNotFoundException("Settings of index '" + id + "', not found");
        }
        configManager.delete(river);
    }

    @PostMapping("/config/{id}/stop")
    public void StopIndex(@PathVariable String id) {
        for (RunningHarvester harvester : ApiServer.indexer.getHarvesterPool()) {
            if (harvester.getIndexName().equals(id)) {
                harvester.stop();
                return;
            }
        }
        throw new SettingNotFoundException("Indexing of index '" + id + "' is not running");
    }


}
