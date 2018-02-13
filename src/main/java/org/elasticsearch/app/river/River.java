package org.elasticsearch.app.river;

import org.elasticsearch.search.SearchHit;

public class River {
    private RiverName riverName;
    private Object riverSettings;

    public River() {
    }

    public River(SearchHit sh) {

    }

    public River riverName(String riverName){
        this.riverName = new RiverName("",riverName);
        return this;
    }

    public River riverSettings(Object settings){
        this.riverSettings =  settings;
        return this;
    }
}
