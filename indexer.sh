#!/bin/bash
pidfile=/var/run/indexer.pid

if [ -f "$pidfile" ]
then
    echo "Indexer already running"
else
    trap "rm -f -- '$pidfile'" EXIT
    echo $$ > "$pidfile"
    echo "Starting indexer"
    source ~/.profile
    cd /usr/src/river.rdf
    /usr/bin/mvn compile > /dev/null
    /usr/bin/mvn exec:java -e -Dexec.mainClass="org.elasticsearch.app.Indexer"
    echo "Finished indexer"
fi
