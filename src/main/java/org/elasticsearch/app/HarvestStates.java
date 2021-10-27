package org.elasticsearch.app;

public enum HarvestStates {

    PREPARING( "Preparing"),
    HARVESTING_ENDPOINT( "Harvesting endpoint"),
    CREATING_MODEL( "Creating model"),
    INDEXING( "Indexing"),
    SWITCHING_TO_NEW_INDEX( "Setting as actual"),
    STOPPING( "Stopping");

    private final String message;

    HarvestStates(String message){
        this.message=message;
    }

    @Override
    public String toString() {
        return message;
    }

}
