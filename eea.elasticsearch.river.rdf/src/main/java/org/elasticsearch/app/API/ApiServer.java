package org.elasticsearch.app.API;

import org.elasticsearch.app.Indexer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.Executors;

@SpringBootApplication
public class ApiServer {
    public static  Indexer indexer;
    public static void main(String[] args) {
         indexer = new Indexer();
        SpringApplication.run(ApiServer.class, args);
    }

}
