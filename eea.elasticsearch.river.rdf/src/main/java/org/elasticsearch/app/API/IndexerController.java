package org.elasticsearch.app.API;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.river.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.net.BindException;
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
    @GetMapping("/threads")
    public ArrayList<String> threads() {
        ArrayList<String> s = new ArrayList<>();
        for (Thread thread : ApiServer.indexer.getThreadPool()) {
            s.add(thread.getName());
        }
        Collections.sort(s);
        return s;
    }

    @PutMapping(path = "/config", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfig(@RequestBody String s) throws JsonProcessingException {
        Map<String, Object> map = new ObjectMapper().readValue(s, Map.class);

        River river = new River(map);

        configManager.add(river);

        return river.riverName();
    }

    @PostMapping("/config/{id}/start")
    public void StartIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            //TODO throw Exception
            return;
        }
        if(threads().contains(river.riverName())){
            //TODO throw Exception
            return;
        }

        ApiServer.indexer.setRivers(river);
        ApiServer.indexer.startIndexing();


    }

    @PostMapping(path = "/configAndIndex", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfigAndStart(@RequestBody String s) throws JsonProcessingException {
        String id = SaveConfig(s);
        StartIndex(id);
        return id;
    }

    @DeleteMapping("/config/{id}")
    public void DeleteIndex(@PathVariable String id) {
        River river = configManager.getRiver(id);
        if (Objects.isNull(river)) {
            //TODO throw Exception
            return;
        }
        configManager.remove(river);
    }


}
