package org.elasticsearch.app.river;

import org.elasticsearch.search.SearchHit;

public class River {
    private RiverName riverName;
    private RiverSettings riverSettings;

    public River() {
    }

    public River(SearchHit sh) {

    }

    public String riverName(){
        return this.riverName.name();
    }

    public River setRiverName(String riverName){
        this.riverName = new RiverName("",riverName);
        return this;
    }

    public River setRiverSettings(RiverSettings settings){
        this.riverSettings =  settings;
        return this;
    }

    public RiverSettings getRiverSettings(){
        return this.riverSettings;
    }
}
