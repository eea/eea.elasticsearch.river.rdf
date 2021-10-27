package org.elasticsearch.app.API;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.app.river.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/")
public class IndexerController {

    private static final Logger logger = LoggerFactory.getLogger(IndexerController.class);

    IndexerController() {
    }

    @GetMapping("/")
    public String hello() {
        return "hello world";
    }

    @PutMapping(path = "/config", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfig(@RequestBody String s) throws JsonProcessingException {
        Map<String, Object> map = new ObjectMapper().readValue(s, Map.class);

        River river = new River(map);

        //TODO: save to file

        return river.riverName();
    }

    @PostMapping("/config/{id}/start")
    public void StartIndex(@PathVariable String id) {

    }

    @PutMapping(path = "/configAndIndex", consumes = MediaType.APPLICATION_JSON_VALUE)
    public String SaveConfigAndStart(@RequestBody String s) throws JsonProcessingException {
        String id = SaveConfig(s);
        StartIndex(id);
        return id;
    }
}
