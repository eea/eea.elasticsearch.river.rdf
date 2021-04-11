FROM java:8
EXPOSE 8080
ADD /target/eea-rdf-river-indexer-2.0.3-altered.jar app.jar
ENTRYPOINT ["java", "-jar", "-Xmx4096M", "-Xms2048M", "app.jar"]
