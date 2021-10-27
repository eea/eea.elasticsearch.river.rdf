package org.elasticsearch.app.debug;

import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.app.support.ESNormalizer;

import java.util.HashMap;

public class JSONMap extends HashMap {
    private final ESLogger logger = Loggers.getLogger(JSONMap.class);


    @Override
    public Object put(Object key, Object value) {
        if(key instanceof String && key.equals("http://www.eea.europa.eu/ontologies.rdf#fleschReadingEaseScore"))
            logger.info("{}:{}", key,value );

        return super.put(key, value);
    }
}
