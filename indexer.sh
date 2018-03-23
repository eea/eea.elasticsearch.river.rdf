#!/bin/bash
source ~/.profile
cd /usr/src/river.rdf
#/usr/bin/mvn compile
/usr/bin/mvn compile > /dev/null
/usr/bin/mvn exec:java -e -Dexec.mainClass="org.elasticsearch.app.Indexer"