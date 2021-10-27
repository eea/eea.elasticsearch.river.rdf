#!/bin/bash
pidfile=/var/run/indexer.pid

echo "Entering indexer"
if [ -f "$pidfile" ]
then
    echo "Indexer already running"
else
    echo "Starting indexer"
    trap "rm -f -- '$pidfile'" EXIT
    echo $$ > "$pidfile"
    source ~/.profile
    cd /usr/src/river.rdf
#    /usr/bin/mvn compile > /dev/null #uncomment for development
    /usr/bin/mvn exec:java -e -Dexec.mainClass="org.elasticsearch.app.Indexer"
    echo "Finished indexer"
fi
