## Instalation

### 1. Pull images for Elasticsearch and Kibana

Execute folowing commands to pull Elasticsearch and Kibana images to docker:

	    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
	    docker pull docker.elastic.co/kibana/kibana:7.10.0

### 2.  Building server application
	
Copy production build of frontend application ([link](https://gitlab.fel.cvut.cz/svagrmic/bp-application)) to *frontend/build* 

Build server aplication:

	    mvn clean install

### 3. Create docker containers

Change settings in *docker-compose.yml* file.

		docker compose up