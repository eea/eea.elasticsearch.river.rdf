# Instalation

## HTTP

### 1.  Building server application

Copy production build of frontend application ([link](https://github.com/opendata-mvcr/dashboard-indexer-frontend)) to *frontend/build*   
Build server aplication:

	 mvn clean install  

### 2. Create docker containers

Change settings in *docker-compose.yml* file. Then create images and start containers with command:

	 docker-compose up -d

## HTTPS

### 1.  Building server application

Copy production build of frontend application ([link](https://gitlab.fel.cvut.cz/svagrmic/bp-application)) to *frontend/build*

Build server aplication:

	mvn clean install

### 2. Create docker containers

Change settings in *docker-compose.yml* file. Then create images and start containers with command:

	docker-compose -f create-certs.yml run --rm create_certs
	docker-compose -f docker-compose-https.yml up -d

Create initial passwords for Elasticsearch

	docker exec es01 /bin/bash -c "bin/elasticsearch-setup-passwords auto --batch --url https://localhost:9200"

Save generated password `elastic` and `kibana_system`. Edit `docker-compose-https.yml` by changing field `ELASTICSEARCH_PASSWORD` to saved password `kibana_system`.

	...
	ELASTICSEARCH_USERNAME: kibana_system  
	ELASTICSEARCH_PASSWORD: **ChangeMe** 
	ELASTICSEARCH_SSL_CERTIFICATEAUTHORITIES: $CERTS_DIR/ca/ca.crt
	...

Recreate kibana containers.

	docker-compose -f docker-compose-https.yml up -d

Login to Kibana (https://localhost:5601) and create you own user with superuser role in `side menu > Stack Management > Users (under Security)`

- Username: elastic
- Password: [saved_elastic_pass]

-----

Nástroj pro indexování RDF dat pro Elasticsearch. Tento repozitář je udržován v rámci projektu OPZ č. [CZ.03.4.74/0.0/0.0/15_025/0013983](https://esf2014.esfcr.cz/PublicPortal/Views/Projekty/Public/ProjektDetailPublicPage.aspx?action=get&datovySkladId=F5E162B2-15EC-4BBE-9ABD-066388F3D412).  
![Evropská unie - Evropský sociální fond - Operační program Zaměstnanost](https://data.gov.cz/images/ozp_logo_cz.jpg)