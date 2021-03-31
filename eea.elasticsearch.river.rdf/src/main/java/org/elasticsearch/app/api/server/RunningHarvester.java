package org.elasticsearch.app.ApiSpringServer;

public interface RunningHarvester {
    void stop();

    String getIndexName();
}
