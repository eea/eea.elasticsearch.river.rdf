FROM maven:3.5.2-jdk-8

ADD . /usr/src/river.rdf
#RUN cd /usr/src/river.rdf && mvn clean install
CMD cd /usr/src/river.rdf &&  mvn clean && mvn compile && mvn exec:java -Dexec.mainClass="org.elasticsearch.app.Indexer"