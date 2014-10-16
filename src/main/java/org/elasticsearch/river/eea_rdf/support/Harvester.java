package org.elasticsearch.river.eea_rdf.support;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.client.Client;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;

import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFLanguages ;
import org.apache.jena.riot.RiotException;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Date;
import java.lang.StringBuffer;
import java.lang.Exception;
import java.lang.Integer;
import java.lang.Byte;
import java.text.SimpleDateFormat;
import java.io.IOException;

import javax.xml.ws.Endpoint;
import javax.xml.ws.http.HTTPException;

/**
 *
 * @author iulia
 *
 */
public class Harvester implements Runnable {

	private final ESLogger logger = Loggers.getLogger(Harvester.class);

	private Boolean indexAll = true;
	private String startTime;
	private Set<String> rdfUrls;
	private String rdfEndpoint;
	private List<String> rdfQueries;
	private String rdfQuery;
	private int rdfQueryType;
	private List<String> rdfPropList;
	private Boolean rdfListType = false;
	private Boolean hasList = false;
	private Map<String, String> normalizeProp;
	private Map<String, String> normalizeObj;
	private Boolean willNormalizeProp = false;
	private Boolean willNormalizeObj = false;
	private Boolean addLanguage = false;
	private String language;
	private List<String> uriDescriptionList;
	private Boolean toDescribeURIs = false;
	private Boolean addUriForResource;
	private Boolean hasBlackMap = false;
	private Boolean hasWhiteMap = false;
	private Map<String,List<String>> blackMap;
	private Map<String,List<String>> whiteMap;
	private String syncConditions;
	private String syncTimeProp;
	private Boolean syncOldData;

	private Client client;
	private String indexName;
	private String typeName;

	private Boolean closed = false;

	/**
 	 * Sets the {@link #Harvester}'s {@link #rdfUrls} parameter
 	 * @param url - a list of urls
 	 * @return the {@link #Harvester} with the {@link #rdfUrls} parameter set
 	 */
	public Harvester rdfUrl(String url) {
		url = url.substring(1, url.length() - 1);
		rdfUrls = new HashSet<String>(Arrays.asList(url.split(",")));
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #rdfEndpoint} parameter
	 * @param endpoint - new endpoint
	 * @return the same {@link #Harvester} with the {@link #rdfEndpoint}
	 * parameter set
	 */
	public Harvester rdfEndpoint(String endpoint) {
		this.rdfEndpoint = endpoint;
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #rdfQuery} parameter
	 * @param query - new list of queries
	 * @return the same {@link #Harvester} with the {@link #rdfQuery} parameter
	 * set
	 */
	public Harvester rdfQuery(List<String> query) {
		if(!query.isEmpty()) {
			this.rdfQueries = new ArrayList<String>(query);
		} else {
			rdfQueries = new ArrayList<String>(query);
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #rdfQueryType} parameter
	 * @param queryType - the type of any possible query
	 * @return the same {@link #Harvester} with the {@link #rdfQueryType}
	 * parameter set
	 */
	public Harvester rdfQueryType(String queryType) {
		if (queryType.equals("select"))
			this.rdfQueryType = 1;
		else if (queryType.equals("construct"))
			this.rdfQueryType = 0;
		else
			this.rdfQueryType = 2;
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #rdfPropList} parameter
	 * @param list - a list of properties names that are either required in
	 * the object description, or undesired, depending on its
	 * {@link #rdfListType}
	 * @return the same {@link #Harvester} with the {@link #rdfPropList}
	 * parameter set
	 */
	public Harvester rdfPropList(List<String> list) {
		if(!list.isEmpty()) {
			hasList = true;
			rdfPropList = new ArrayList<String>(list);
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #rdfListType} parameter
	 * @param listType - a type ("black" or "white") for the {@link #rdfPropList}
	 * in case it exists
	 * @return the same {@link #Harvester} with the {@link #rdfListType}
	 * parameter set
	 * @Observation A blacklist contains properties that should not be indexed
	 * with the data while a whitelist contains all the properties that should
	 * be indexed with the data.
	 */
	public Harvester rdfListType(String listType) {
		if(listType.equals("white"))
			this.rdfListType = true;
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #addLanguage} parameter.
	 * @param rdfAddLanguage - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #addLanguage}
	 * parameter set
	 * @Observation When "addLanguage" is set on "true", all the languages
	 * of the String Literals will be included in the output of a new property,
	 * "language".
	 */
	public Harvester rdfAddLanguage(Boolean rdfAddLanguage) {
		this.addLanguage = rdfAddLanguage;
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #language} parameter. The default
	 * value is 'en"
	 * @param rdfLanguage - new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #language} parameter
	 * set
	 */
	public Harvester rdfLanguage(String rdfLanguage) {
		this.language = rdfLanguage;
		if(!this.language.isEmpty()){
			this.addLanguage = true;
			if(!this.language.startsWith("\""))
				this.language = "\"" +  this.language + "\"";
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #normalizeProp} parameter.
	 * {@link #normalizeProp} contains pairs of property-replacement. The
	 * properties are replaced with the given values and if one resource has
	 * both properties their values are grouped in a list.
	 *
	 * @param normalizeProp - new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #normalizeProp}
	 * parameter set
	 * @Observation In case there is at least one property, the
	 * {@link #willNormalizeProp} parameter is set to true.
	 */
	public Harvester rdfNormalizationProp(Map<String, String> normalizeProp) {
		if(normalizeProp != null || !normalizeProp.isEmpty()) {
			willNormalizeProp = true;
			this.normalizeProp = normalizeProp;
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #normalizeObj} parameter.
	 * {@link #normalizeObj} contains pairs of object-replacement. Objects
	 * are replaced with given values no matter of the property whose value
	 * they represent.
	 *
	 * @param normalizeObj - new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #normalizeObj}
	 * parameter set
	 * @Observation In case there is at least one object to be normalized, the
	 * {@link #willNormalizeObj} parameter is set to true
	 */
	public Harvester rdfNormalizationObj(Map<String, String> normalizeObj) {
		if(normalizeObj != null || !normalizeObj.isEmpty()) {
			willNormalizeObj = true;
			this.normalizeObj = normalizeObj;
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #blackMap} parameter. A blackMap
	 * contains all the pairs property - list of objects that are not meant to
	 * be indexed.
	 *
	 * @param blackMap - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #blackMap}
	 * parameter set
	 */
	public Harvester rdfBlackMap(Map<String,Object> blackMap) {
		if(blackMap != null || !blackMap.isEmpty()) {
			hasBlackMap = true;
			this.blackMap =  new HashMap<String,List<String>>();
			for(Map.Entry<String,Object> entry : blackMap.entrySet()) {
				this.blackMap.put(
					entry.getKey(), (List<String>)entry.getValue());
			}
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #whiteMap} parameter.  A whiteMap
	 * contains all the pairs property - list of objects that are meant to be
	 * indexed.
	 *
	 * @param whiteMap - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #whiteMap}
	 * parameter set
	 */
	public Harvester rdfWhiteMap(Map<String,Object> whiteMap) {
		if(whiteMap != null || !whiteMap.isEmpty()) {
			hasWhiteMap = true;
			this.whiteMap =  new HashMap<String,List<String>>();
			for(Map.Entry<String,Object> entry : whiteMap.entrySet()) {
				this.whiteMap.put(
					entry.getKey(), (List<String>)entry.getValue());
			}
		}
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #uriDescriptionList} parameter.
	 * Whenever {@link #uriDescriptionList} is set, all the objects represented
	 * by URIs are replaced with the resource's label. The label is the first
	 * of the properties in the given list, for which the resource has an object.
	 *
	 * @param uriList - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #uriDescriptionList}
	 * parameter set
	 *
	 * @Observation If the list is not empty, the {@link #toDescribeURIs}
	 * property is set to true
	 */
	public Harvester rdfURIDescription(String uriList) {
		uriList = uriList.substring(1, uriList.length() - 1);
		if(!uriList.isEmpty())
			toDescribeURIs = true;
		uriDescriptionList = Arrays.asList(uriList.split(","));
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #uriDescriptionList} parameter.
	 * When it is set to true  a new property is added to each resource:
	 * {@link http://www.w3.org/1999/02/22-rdf-syntax-ns#about}, having the value
	 * equal to the resource's URI.
	 *
	 * @param rdfAddUriForResource - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #addUriForResource}
	 * parameter set
	 */
	public Harvester rdfAddUriForResource(Boolean rdfAddUriForResource) {
		this.addUriForResource = rdfAddUriForResource;
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #syncConditions} parameter. It
	 * represents the sync query's additional conditions for indexing. These
	 * conditions are added to the time filter.
	 * @param syncCond - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #syncConditions}
	 * parameter set
	 */
	public Harvester rdfSyncConditions(String syncCond) {
		this.syncConditions = syncCond;
		if(!syncCond.isEmpty())
			this.syncConditions += " . ";
		return this;
	}

	/**
	 * Sets the {@link #Harvester}'s {@link #syncTimeProp} parameter. It
	 * represents the sync query's time parameter used when filtering the
	 * endpoint's last updates.
	 * @param syncTimeProp - a new value for the parameter
	 * @return the same {@link #Harvester} with the {@link #syncTimeProp}
	 * parameter set
	 */
	public Harvester rdfSyncTimeProp(String syncTimeProp) {
		this.syncTimeProp = syncTimeProp;
		return this;
	}

    /**
     * Sets the {@link #Harvester}'s {@link #syncOldData} parameter. When this
     * parameter is set to true, the endpoint will be queried again without the
     * {@link #syncConditions} to update existing resources that were changed.
     * THe default value is true
     * @param syncOldData - a new value for the parameter
     * return the same {@link #Harvester} with the {@link #syncOldData}
     * parameter set
     */
	public Harvester rdfSyncOldData(Boolean syncOldData) {
		this.syncOldData = syncOldData;
		return this;
	}

	public Harvester client(Client client) {
		this.client = client;
		return this;
	}

	public Harvester index(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public Harvester type(String typeName) {
		this.typeName = typeName;
		return this;
	}

	public Harvester rdfIndexType(String indexType) {
		if (indexType.equals("sync"))
			this.indexAll = false;
		return this;
	}

	public Harvester rdfStartTime(String startTime) {
		this.startTime = startTime;
		return this;
	}

	public void log(String message) {
		logger.info(message);
	}

	public void setClose(Boolean value) {
		this.closed = value;
	}

	@Override
	public void run() {
		long currentTime = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date now = new Date(currentTime);

		if(indexAll)
			runIndexAll();
		else
			runSync();

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		try {
			bulkRequest.add(client.prepareIndex(indexName, "stats", "1")
					.setSource(jsonBuilder()
						.startObject()
						.field("last_update", sdf.format(now))
					.endObject()));
		} catch (IOException ioe) {
			logger.error("Could not add the stats to ES. {}",
						 ioe.getLocalizedMessage());
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		return ;
	}

	public void runSync() {
		logger.info(
				"Starting RDF synchronizer: from [{}], endpoint [{}], " +
				"index name [{}], type name [{}]",
				startTime, rdfEndpoint,	indexName, typeName);

		while(true) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			Date lastUpdate = new Date(System.currentTimeMillis());

			if(this.closed){
				logger.info(
						"Ended synchronization from [{}], for endpoint [{}]," +
						"at index name {}, type name {}",
						lastUpdate, rdfEndpoint, indexName, typeName);
				return;
			}
			/**
			 * Synchronize with the endpoint
			 */

			if(startTime.isEmpty()) {
				GetResponse response = client
					.prepareGet(indexName, "stats", "1")
					.setFields("last_update")
					.execute()
					.actionGet();
				startTime = (String)response.getField("last_update").getValue();
			}

			try {
				lastUpdate = sdf.parse(startTime);
			} catch (Exception e){
				logger.error("Could not parse time. [{}]", e.getLocalizedMessage());
			}

			sync();
			closed = true;
		}

	}

	/**
	 * Starts a harvester with predefined queries to synchronize with the
	 * changes from the SPARQL endpoint
	 */
	public void sync() {
		rdfQuery = "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
			"SELECT ?resource WHERE { " +
			"?resource <" + this.syncTimeProp + "> ?time ." +
			this.syncConditions +
			" FILTER (?time > xsd:dateTime(\"" + startTime + "\")) }";

		rdfUrls = new HashSet<String>();

		try {
			Query query = QueryFactory.create(rdfQuery);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(
					rdfEndpoint, query);
			try {
				ResultSet results = qexec.execSelect();

				while(results.hasNext()) {
					QuerySolution sol = results.nextSolution();
					try {
						String value = sol.getResource("resource").toString();
						if (value.contains("login_form")) {
							continue;
						}
						if (value.endsWith("/@@rdf")) {
							value = value.substring(0, value.length() - 6);
						}
						rdfUrls.add(value);
					} catch (NoSuchElementException nsee) {
						logger.error(
							"Encountered a NoSuchElementException: " +
							nsee.getLocalizedMessage());
					}
				}
			} catch (Exception e) {
				logger.error(
						"Encountered a [{}] while querying the endpoint for sync",
						e.getLocalizedMessage());
			} finally {
				qexec.close();
			}
		} catch (QueryParseException qpe) {
			logger.warn(
					"Could not parse [{}]. Please provide a relevant quey. {}",
					rdfQuery, qpe.getLocalizedMessage());
		}
		int count = rdfUrls.size();

		/**
		 * If desired, query for old data that has the sync conditions modified
		 *
		 * This option is useful in the case in which the application indexes
		 * resources that match some conditions. In this case, if they are
		 * modified and no longer match the initial conditions, they will not
		 * be synchronized. When syncOldData is True, the modified resources
		 * that no longer match the conditions are deleted.
		 *
		 *
		 */
		if (this.syncOldData) {
			rdfQuery = "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
			"SELECT ?resource WHERE { " +
			"?resource <" + this.syncTimeProp + "> ?time ." +
			" FILTER (?time > xsd:dateTime(\"" + startTime + "\")) }";
			try {
				Query query = QueryFactory.create(rdfQuery);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(
						rdfEndpoint, query);
				try {
					ResultSet results = qexec.execSelect();
					while(results.hasNext()) {
						QuerySolution sol = results.nextSolution();
						String value = sol.getResource("resource").toString();
						if (value.contains("login_form")) {
							continue;
						}
						if (value.endsWith("/@@rdf")) {
							value = value.substring(0, value.length() - 6);
						}
						if (!rdfUrls.contains(value)) {
							//delete the resource if it exists
							logger.info("Deleting old resource: [{}] ", value);

							DeleteResponse response = client.prepareDelete(
															indexName,
															typeName,
															value)
        												.execute()
        												.actionGet();
        					count++;
						}
					}
				} catch (NoSuchElementException nsee) {
					logger.error("Error when querying without conditions. {} ",
								 nsee.getLocalizedMessage());
				} catch (Exception e) {
					logger.error(
						"Error while querying for sync, without conditions. {}",
						e.getLocalizedMessage());
				} finally {
					qexec.close();
				}
			} catch (QueryParseException qpe) {
				logger.warn(
					"Could not parse [{}]. Please provide a relevant quey {}",
					rdfQuery, qpe.getLocalizedMessage());
			}

		}

		for (String uri : rdfUrls) {
			//make query for each/all entries
			switch(rdfQueryType) {
				case 0: rdfQuery = "CONSTRUCT {<" + uri + "> ?p ?o WHERE { <" +
								uri + "> ?p ?o}";
								break;
				case 1: rdfQuery = "SELECT <" + uri + "> as ?s ?p ?o WHERE { <" +
								uri + "> ?p ?o}";
								break;
				case 2: rdfQuery = "DESCRIBE <" + uri + ">";
								break;
				default: rdfQuery = "";
								 break;
			}
			rdfQuery = "DESCRIBE <" + uri + ">";
			try {
				Query query = QueryFactory.create(rdfQuery);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(
						rdfEndpoint, query);
				try {
					Model constructModel = ModelFactory.createDefaultModel();

					qexec.execConstruct(constructModel);

					if (constructModel.isEmpty() && !indexAll) {
						//delete the resource
						logger.info("Deleting [{}] ", uri);

						DeleteResponse response = client.prepareDelete(
							indexName,
							typeName,
							uri)
        												.execute()
        												.actionGet();
					}

					BulkRequestBuilder bulkRequest = client.prepareBulk();
					addModelToES(constructModel, bulkRequest);
				} catch (Exception e) {
					logger.error(
						"Error while querying for modified content. {}",
						e.getLocalizedMessage());
				} finally {	qexec.close(); }
			} catch (QueryParseException  qpe) {
				logger.warn(
					"Could not parse [{}]. Please provide a relevant query. {}",
					rdfQuery, qpe.getLocalizedMessage());
			}

		}
		logger.info("Finished synchronisation for {} resources ", count);
	}

	/**
	 * Starts the harvester for queries and/or URLs
	 */
	public void runIndexAll() {

		logger.info(
				"Starting RDF harvester: endpoint [{}], queries [{}]," +
				"URLs [{}], index name [{}], typeName [{}]",
 				rdfEndpoint, rdfQueries, rdfUrls, indexName, typeName);

		while (true) {
			if(this.closed){
				logger.info("Ended harvest for endpoint [{}], queries [{}]," +
						"URLs [{}], index name {}, type name {}",
						rdfEndpoint, rdfQueries, rdfUrls, indexName, typeName);

				return;
			}

			/**
			 * Harvest from a SPARQL endpoint
			 */
			if(!rdfQueries.isEmpty()) {
				harvestFromEndpoint();
			}

			/**
			 * Harvest from RDF dumps
			 */
			harvestFromDumps();

			closed = true;
		}
	}

	/**
	 * Queries the {@link #rdfEndpoint(String)} with the {@link #rdfQuery(String)}
	 * and harvests the results of the query. The query should only return triples,
	 * named 's', 'p' and 'o'
	 * @param qexec a SELECT query
 	 */
	private void harvestWithSelect(QueryExecution qexec) {
		Model sparqlModel = ModelFactory.createDefaultModel();
		Graph graph = sparqlModel.getGraph();
		boolean got500 = true;

		while(got500) {
			try {
				ResultSet results = qexec.execSelect();

				while(results.hasNext()) {
					QuerySolution sol = results.nextSolution();
					/**
				 	* Each QuerySolution is a triple
				 	*/
					try {
						String subject = sol.getResource("s").toString();
						String predicate = sol.getResource("p").toString();
						String object = sol.get("o").toString();

						graph.add(new Triple(
									NodeFactory.createURI(subject),
									NodeFactory.createURI(predicate),
									NodeFactory.createLiteral(object)));

					} catch(NoSuchElementException nsee) {
						logger.error("Could not index [{}]: Query result was" +
								"not a triple. {}",	sol.toString(),
								nsee.getLocalizedMessage());
					}
				}
				got500 = false;
			} catch(Exception e) {
				String errorText = e.toString();
				if(e instanceof QueryExceptionHTTP &&
					errorText.contains("Internal Server Error")) {
					got500 = true;
					logger.warn(
						"The endpoint replied with an internal error. Retrying");
				} else {
					got500 = false;
					logger.error(
						"Encountered a [{}] when quering the endpoint",
						errorText);
				}
			} finally { qexec.close(); }
		}

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		addModelToES(sparqlModel, bulkRequest);
	}

	/**
	 * Queries the {@link #rdfEndpoint(String)} with the {@link #rdfQuery(String)}
	 * and harvests the results of the query.
	 * @param qexec a CONSTRUCT query
	 */
	private void harvestWithConstruct(QueryExecution qexec) {
		Model sparqlModel = ModelFactory.createDefaultModel();
		boolean got500 = true;

		while(got500) {
			try {
				qexec.execConstruct(sparqlModel);
				got500 = false;
			} catch (Exception e) {
				String errorText = e.toString();
				if(e instanceof QueryExceptionHTTP &&
					errorText.contains("Internal Server Error")) {
					got500 = true;
					logger.warn(
						"The endpoint replied with an internal error. Retrying");
				} else {
					got500 = false;
					logger.error(
						"Encountered a [{}] when quering the endpoint",
						errorText);
				}
			}

			BulkRequestBuilder bulkRequest = client.prepareBulk();
			addModelToES(sparqlModel, bulkRequest);
		}
		qexec.close();
	}

	/**
	 * Queries the {@link #rdfEndpoint(String)} with the {@link #rdfQuery(String)}
	 * and harvests the results of the query.
	 *
	 * Observation: At this time only the CONSTRUCT and SELECT queries are
	 * supported
	 */
	private void harvestFromEndpoint() {

		Query query = null;
		QueryExecution qexec = null;

		for (String rdfQuery:rdfQueries) {
			logger.info(
				"Harvesting with query: [{}] on index [{}] and type [{}]",
 				rdfQuery, indexName, typeName);
			try {
				query = QueryFactory.create(rdfQuery);
				qexec = QueryExecutionFactory.sparqlService(
						rdfEndpoint,
						query);
				switch(rdfQueryType) {
					case 0: harvestWithConstruct(qexec);
								break;
					case 1: harvestWithSelect(qexec);
								break;
					case 2: //TODO implement harvestWithDescribe
								break;
					default: break;
				}

			} catch (QueryParseException qpe) {
				logger.error(
						"Could not parse [{}]. Please provide a relevant query. {}",
						rdfQuery, qpe);
			}
		}
	}

	/**
	 * Harvests all the triplets from each URI in the @rdfUrls list
	 */
	private void harvestFromDumps() {
		for(String url:rdfUrls) {
			if(url.isEmpty()) continue;

			logger.info("Harvesting url [{}]", url);

			Model model = ModelFactory.createDefaultModel();
			try {
				RDFDataMgr.read(model, url.trim(), RDFLanguages.RDFXML);
				BulkRequestBuilder bulkRequest = client.prepareBulk();

				addModelToES(model, bulkRequest);
			}
			catch (RiotException re) {
				logger.error("Illegal xml character [{}]", re.getLocalizedMessage());
			}
			catch (Exception e) {
				logger.error("Exception when harvesting url: {}. Details: {}",
					url, e.getLocalizedMessage());
			}
		}
	}

	/**
	 * Index all the resources in a Jena Model to ES
	 *
	 * @param model the model to index
	 * @param bulkRequest a BulkRequestBuilder
	 */
	private void addModelToES(Model model, BulkRequestBuilder bulkRequest) {
		HashSet<Property> properties = new HashSet<Property>();

		StmtIterator iter = model.listStatements();

		while(iter.hasNext()) {
			Statement st = iter.nextStatement();
			Property prop = st.getPredicate();
			String property = prop.toString();

			if(!hasList
				|| (rdfListType && rdfPropList.contains(property))
				|| (!rdfListType && !rdfPropList.contains(property))
				|| (willNormalizeProp && normalizeProp.containsKey(property))) {
				properties.add(prop);
			}
		}

		ResIterator rsiter = model.listSubjects();

		while(rsiter.hasNext()){

			Resource rs = rsiter.nextResource();
			Map<String, ArrayList<String>> jsonMap = new HashMap<String,
				ArrayList<String>>();
			ArrayList<String> results = new ArrayList<String>();
			if(addUriForResource) {
				results.add("\"" + rs.toString() + "\"");
				jsonMap.put(
						"http://www.w3.org/1999/02/22-rdf-syntax-ns#about",
						results);
			}

			Set<String> rdfLanguages = new HashSet<String>();

			for(Property prop: properties) {
				NodeIterator niter = model.listObjectsOfProperty(rs,prop);
				String property = prop.toString();
				if(niter.hasNext()) {
					results = new ArrayList<String>();
					String lang = "";
					String currValue = "";

					while(niter.hasNext()) {
						RDFNode node = niter.next();
						currValue = getStringForResult(node);
						if(addLanguage){
							try {
								lang = node.asLiteral().getLanguage();
								if(!lang.isEmpty()) {
									rdfLanguages.add("\"" + lang + "\"");
								}
							} catch (Exception e) {}
						}

						String shortValue = currValue;
						int currlen = currValue.length();
						if(currlen > 1)
							shortValue = currValue.substring(
												1,
												currlen - 1);

						if((hasWhiteMap && whiteMap.containsKey(property) &&
								!whiteMap.get(property).contains(shortValue)) ||
							 (hasBlackMap && blackMap.containsKey(property) &&
								blackMap.get(property).contains(shortValue))) {
								continue;
						} else {
							if(willNormalizeObj &&
								normalizeObj.containsKey(shortValue)) {
								results.add("\"" +
									normalizeObj.get(shortValue) + "\"");
							} else {
									results.add(currValue);
							}
						}
					}

					if(!results.isEmpty()) {
						if(willNormalizeProp &&
							normalizeProp.containsKey(property)) {
							property = normalizeProp.get(property);
							if(jsonMap.containsKey(property)) {
								results.addAll(jsonMap.get(property));
								jsonMap.put(property, results);
							} else {
								jsonMap.put(property, results);
							}
						} else {
							jsonMap.put(property,results);
						}
					}
				}
			}
			if(addLanguage) {
				if(rdfLanguages.isEmpty() && !language.isEmpty())
					rdfLanguages.add(language);
				if(!rdfLanguages.isEmpty())
					jsonMap.put(
						"language", new ArrayList<String>(rdfLanguages));
			}

			bulkRequest.add(client.prepareIndex(indexName, typeName,
							rs.toString())
				.setSource(mapToString(jsonMap)));
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		}
	}

	/**
	 * Converts a map of results to a String JSON representation for it
	 * @param map a map that matches properties with an ArrayList of
	 * values
	 * @return the JSON representation for the map, as a String
	 */
	private String mapToString(Map<String, ArrayList<String>> map) {
		StringBuffer result = new StringBuffer("{");
		for(Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
			ArrayList<String> value = entry.getValue();
			if(value.size() == 1)
				result.append("\"" + entry.getKey() + "\" : " +
					value.get(0) + ",\n");
			else
				result.append("\"" + entry.getKey() + "\" : " +
					value.toString() + ",\n");
		}
		result.setCharAt(result.length() - 2, '}');
		return result.toString();
	}

	/**
	 * Builds a String result for Elastic Search from an RDFNode
	 * @param node An RDFNode representing the value of a property for a given
	 * resource
	 * @return If the RDFNode has a Literal value, among Boolean, Byte, Double,
	 * Float, Integer Long, Short, this value is returned, converted to String
	 * <p>If the RDFNode has a String Literal value, this value will be
	 * returned, surrounded by double quotes </p>
	 * <p>If the RDFNode has a Resource value (URI) and toDescribeURIs is set
	 * to true, the value of @getLabelForUri for the resource is returned,
	 * surrounded by double quotes.</p>
	 * Otherwise, the URI will be returned
	 */
	private String getStringForResult(RDFNode node) {
		String result = "";
		boolean quote = false;

		if(node.isLiteral()) {
			Object literalValue = node.asLiteral().getValue();
			try {
				Class<?> literalJavaClass = node.asLiteral()
					.getDatatype()
					.getJavaClass();

				if(literalJavaClass.equals(Boolean.class)
						|| literalJavaClass.equals(Byte.class)
						|| literalJavaClass.equals(Double.class)
						|| literalJavaClass.equals(Float.class)
						|| literalJavaClass.equals(Integer.class)
						|| literalJavaClass.equals(Long.class)
						|| literalJavaClass.equals(Short.class)) {

					result += literalValue;
				}	else {
					result =	EEASettings.parseForJson(
							node.asLiteral().getLexicalForm());
					quote = true;
				}
			} catch (java.lang.NullPointerException npe) {
				result = EEASettings.parseForJson(
						node.asLiteral().getLexicalForm());
				quote = true;
			}

		} else if(node.isResource()) {
			result = node.asResource().getURI();
			if(toDescribeURIs) {
				result = getLabelForUri(result);
			}
			quote = true;
		}
		if(quote) {
			result = "\"" + result + "\"";
		}
		return result;
	}


	/**
	 * Returns the string value of the first of the properties in the
	 * uriDescriptionList for the given resource (as an URI). In case the
	 * resource does not have any of the properties mentioned, its URI is
	 * returned. The value is obtained by querying the endpoint and the
	 * endpoint is queried repeatedly until it gives a response (value or the
	 * lack of it)
	 *
	 * It is highly recommended that the list contains properties like labels
	 * or titles, with test values.
	 *
	 * @param uri - the URI for which a label is required
	 * @return a String value, either a label for the parameter or its value
	 * if no label is obtained from the endpoint
	 */
	private String getLabelForUri(String uri) {
		String result = "";
		for(String prop:uriDescriptionList) {
			String innerQuery = "SELECT ?r WHERE {<" + uri + "> <" +
				prop + "> ?r } LIMIT 1";

			try {
				Query query = QueryFactory.create(innerQuery);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(
						rdfEndpoint,
						query);
				boolean keepTrying = true;
				while(keepTrying) {
					keepTrying = false;
					try {
						ResultSet results = qexec.execSelect();

						if(results.hasNext()) {
							QuerySolution sol = results.nextSolution();
							result = EEASettings.parseForJson(
									sol.getLiteral("r").getLexicalForm());
							if(!result.isEmpty())
								return result;
						}
					} catch(Exception e){
						keepTrying = true;
						logger.warn("Could not get label for uri {}. Retrying.",
									uri);
					}finally { qexec.close();}
				}
			} catch (QueryParseException qpe) {
				logger.error("Exception for query {}. The label cannot be obtained",
							 innerQuery);
			}
		}
		return uri;
	}
}
