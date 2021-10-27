## Instalation
1. Create docker container with Elasticsearch

	    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
	    docker run --name ElasticSearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.0
	
2. Create docker container with Kibana

	    docker pull docker.elastic.co/kibana/kibana:7.10.0
	    docker run --name Kibana --link ElasticSearch:elasticsearch -p 5601:5601 docker.elastic.co/kibana/kibana:7.10.0

3. Create docker container with EEA river indexer

    Change settings in docker-compose.yml file. Execute in directory eea.elasticsearch.river.rdf

	    docker-compose up

##