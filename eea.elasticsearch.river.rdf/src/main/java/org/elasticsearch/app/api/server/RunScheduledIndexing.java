package org.elasticsearch.app.ApiSpringServer;

import org.elasticsearch.app.ApiServer;
import org.elasticsearch.app.river.River;

public class RunScheduledIndexing implements Runnable {
    private River river;

    public RunScheduledIndexing(River river) {
        this.river = river;
    }

    @Override
    public void run() {
        if (ApiServer.indexer.getHarvesterPool().stream().anyMatch(h -> h.getIndexName().equals(river.riverName())))
            return;
        ApiServer.indexer.setRivers(river);
        ApiServer.indexer.startIndexing();
    }
}
