package org.elasticsearch.app.api.server;

public interface RunningHarvester {

    void stop();

    String getIndexName();
}
