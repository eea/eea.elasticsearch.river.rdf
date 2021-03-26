package org.elasticsearch.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApiServer {
    public static  Indexer indexer;
    public static void main(String[] args) {
         indexer = new Indexer();
        SpringApplication.run(ApiServer.class, args);
    }

}
