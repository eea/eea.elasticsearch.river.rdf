package org.elasticsearch.app;

public enum HarvestStates {
    PREPARING{
        @Override
        public String toString() {
            return "Preparing";
        }
    },
    HARVESTING_ENDPOINT{
        @Override
        public String toString() {
            return "Harvesting endpoint";
        }
    },
    CREATING_MODEL{
        @Override
        public String toString() {
            return "Creating model";
        }
    },
    INDEXING{
        @Override
        public String toString() {
            return "Indexing";
        }
    },
    SWITCHING_TO_NEW_INDEX{
        @Override
        public String toString() {
            return "Setting as actual";
        }
    },
    STOPPING{
        @Override
        public String toString() {
            return "Stopping";
        }
    }
}
