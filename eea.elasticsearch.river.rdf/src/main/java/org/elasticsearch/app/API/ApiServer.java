package org.elasticsearch.app.API;

import org.elasticsearch.app.Indexer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApiServer {
    public static void main(String[] args) {
        //Indexer indexer = new Indexer();
        SpringApplication.run(ApiServer.class, args);
    }

}
