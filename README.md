## Instalation
1. Pull images for Elasticsearch and Kibana

	    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
	    docker pull docker.elastic.co/kibana/kibana:7.10.0


2. Create docker containers

    **Change settings in docker-compose.yml file. Execute in directory RDF-Indexer.**

	    mvn package
		docker-compose up