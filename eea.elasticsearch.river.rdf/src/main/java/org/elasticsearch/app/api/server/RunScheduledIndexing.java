package org.elasticsearch.app.api.server;

import org.elasticsearch.app.Indexer;
import org.elasticsearch.app.river.River;


public class RunScheduledIndexing implements Runnable {

    private final River river;

    private final Indexer indexer;

    public RunScheduledIndexing(River river, Indexer indexer) {
        this.river = river;
        this.indexer = indexer;
    }

    @Override
    public void run() {
        if (indexer.getHarvesterPool().stream().anyMatch(h -> h.getIndexName().equals(river.getRiverName())))
            return;
        indexer.setRivers(river);
        indexer.startIndexing();
    }
}
