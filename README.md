## Instalation

### 1. Pull images for Elasticsearch and Kibana

Execute folowing commands to pull Elasticsearch and Kibana images to docker:

	    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
	    docker pull docker.elastic.co/kibana/kibana:7.10.0

### 2.  Building server application
	
Copy production build of frontend application ([link](https://github.com/opendata-mvcr/dashboard-indexer-frontend)) to *frontend/build* 

Build server aplication:

	    mvn clean install

### 3. Create docker containers

Change settings in *docker-compose.yml* file.

		docker compose up

-----		
Nástroj pro indexování RDF dat pro Elasticsearch. Tento repozitář je udržován v rámci projektu OPZ č. [CZ.03.4.74/0.0/0.0/15_025/0013983](https://esf2014.esfcr.cz/PublicPortal/Views/Projekty/Public/ProjektDetailPublicPage.aspx?action=get&datovySkladId=F5E162B2-15EC-4BBE-9ABD-066388F3D412).
![Evropská unie - Evropský sociální fond - Operační program Zaměstnanost](https://data.gov.cz/images/ozp_logo_cz.jpg)