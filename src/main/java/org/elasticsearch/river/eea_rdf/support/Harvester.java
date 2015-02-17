package org.elasticsearch.river.eea_rdf.support;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 * @author iulia
 *
 */
public class Harvester implements Runnable {

	private enum QueryType {
		SELECT,
		CONSTRUCT,
		DESCRIBE
	}

	private final ESLogger logger = Loggers.getLogger(Harvester.class);

	private Boolean indexAll = true;
	private String startTime;
	private Set<String> rdfUrls;
	private String rdfEndpoint;
	private List<String> rdfQueries;
	private QueryType rdfQueryType;
	private List<String> rdfPropList;
	private Boolean rdfListType = false;
	private Boolean hasList = false;
	private Map<String, String> normalizeProp;
	private Map<String, String> normalizeObj;
	private Map<String, String> normalizeMissing;
	private Boolean willNormalizeProp = false;
	private Boolean willNormalizeObj = false;
	private Boolean addLanguage = false;
	private String language;
	private List<String> uriDescriptionList;
	private Boolean toDescribeURIs = false;
	private Boolean addUriForResource;
	private Boolean hasBlackMap = false;
	private Boolean hasWhiteMap = false;
	private Boolean willNormalizeMissing = false;
	private Map<String,Set<String>> blackMap;
	private Map<String,Set<String>> whiteMap;
	private String syncConditions;
	private String syncTimeProp;
	private Boolean syncOldData;

	private Client client;
	private String indexName;
	private String typeName;

	private Boolean closed = false;

	private HashMap<String, String> uriLabelCache;

	/**
 	 * Sets the {@link Harvester}'s {@link #rdfUrls} parameter
 	 * @param url - a list of urls
 	 * @return the {@link Harvester} with the {@link #rdfUrls} parameter set
 	 */
	public Harvester rdfUrl(String url) {
		url = url.substring(1, url.length() - 1);
		uriLabelCache = new HashMap<String, String>();
		rdfUrls = new HashSet<String>(Arrays.asList(url.split(",")));
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfEndpoint} parameter
	 * @param endpoint - new endpoint
	 * @return the same {@link Harvester} with the {@link #rdfEndpoint}
	 * parameter set
	 */
	public Harvester rdfEndpoint(String endpoint) {
		rdfEndpoint = endpoint;
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfQuery} parameter
	 * @param query - new list of queries
	 * @return the same {@link Harvester} with the {@link #rdfQuery} parameter
	 * set
	 */
	public Harvester rdfQuery(List<String> query) {
		rdfQueries = new ArrayList<String>(query);
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfQueryType} parameter
	 * @param queryType - the type of any possible query
	 * @return the same {@link Harvester} with the {@link #rdfQueryType}
	 * parameter set
	 */
	public Harvester rdfQueryType(String queryType) {
		try {
			rdfQueryType = QueryType.valueOf(queryType.toUpperCase());
		} catch (IllegalArgumentException e) {
			logger.error("Bad query type: {}", queryType);

			/* River process can't continue */
			throw e;
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfPropList} parameter
	 * @param list - a list of properties names that are either required in
	 * the object description, or undesired, depending on its
	 * {@link #rdfListType}
	 * @return the same {@link Harvester} with the {@link #rdfPropList}
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
	 * Sets the {@link Harvester}'s {@link #rdfListType} parameter
	 * @param listType - a type ("black" or "white") for the {@link #rdfPropList}
	 * in case it exists
	 * @return the same {@link Harvester} with the {@link #rdfListType}
	 * parameter set
	 * @Observation A blacklist contains properties that should not be indexed
	 * with the data while a whitelist contains all the properties that should
	 * be indexed with the data.
	 */
	public Harvester rdfListType(String listType) {
		if (listType.equals("white"))
			rdfListType = true;
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #addLanguage} parameter.
	 * @param rdfAddLanguage - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #addLanguage}
	 * parameter set
	 * @Observation When "addLanguage" is set on "true", all the languages
	 * of the String Literals will be included in the output of a new property,
	 * "language".
	 */
	public Harvester rdfAddLanguage(Boolean rdfAddLanguage) {
		addLanguage = rdfAddLanguage;
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #language} parameter. The default
	 * value is 'en"
	 * @param rdfLanguage - new value for the parameter
	 * @return the same {@link Harvester} with the {@link #language} parameter
	 * set
	 */
	public Harvester rdfLanguage(String rdfLanguage) {
		language = rdfLanguage;
		if(!language.isEmpty()) {
			addLanguage = true;
			/* Quote the language str */
			if(!language.startsWith("\""))
				language = "\"" +  this.language + "\"";
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #normalizeProp} parameter.
	 * {@link #normalizeProp} contains pairs of property-replacement. The
	 * properties are replaced with the given values and if one resource has
	 * both properties their values are grouped in a list.
	 *
	 * @param normalizeProp - new value for the parameter
	 * @return the same {@link Harvester} with the {@link #normalizeProp}
	 * parameter set
	 * @Observation In case there is at least one property, the
	 * {@link #willNormalizeProp} parameter is set to true.
	 */
	public Harvester rdfNormalizationProp(Map<String, String> normalizeProp) {
		if(normalizeProp != null || !normalizeProp.isEmpty()) {
			this.willNormalizeProp = true;
			this.normalizeProp = normalizeProp;
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #normalizeObj} parameter.
	 * {@link #normalizeObj} contains pairs of object-replacement. Objects
	 * are replaced with given values no matter of the property whose value
	 * they represent.
	 *
	 * @param normalizeObj - new value for the parameter
	 * @return the same {@link Harvester} with the {@link #normalizeObj}
	 * parameter set
	 * @Observation In case there is at least one object to be normalized, the
	 * {@link #willNormalizeObj} parameter is set to true
	 */
	public Harvester rdfNormalizationObj(Map<String, String> normalizeObj) {
		if(normalizeObj != null || !normalizeObj.isEmpty()) {
			this.willNormalizeObj = true;
			this.normalizeObj = normalizeObj;
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #normalizeMissing} parameter.
	 * {@link #normalizeMissing} contains pairs of property-value. Missing
	 * properties are indexed with the given value.
	 *
	 * @param normalizeMissing - new value for the parameter
	 * @return the same {@link Harvester} with the {@link #normalizeMissing}
	 * parameter set
	 * @Observation In case there is at least one object to be normalized, the
	 * {@link #willNormalizeMissing} parameter is set to true
	 */
	public Harvester rdfNormalizationMissing(Map<String, String> normalizeMissing) {
		if(normalizeMissing != null || !normalizeMissing.isEmpty()) {
			this.willNormalizeMissing = true;
			this.normalizeMissing = normalizeMissing;
		}
		return this;
	}
	
	/**
	 * Sets the {@link Harvester}'s {@link #blackMap} parameter. A blackMap
	 * contains all the pairs property - list of objects that are not meant to
	 * be indexed.
	 *
	 * @param blackMap - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #blackMap}
	 * parameter set
	 */
	public Harvester rdfBlackMap(Map<String,Object> blackMap) {
		if(blackMap != null || !blackMap.isEmpty()) {
			hasBlackMap = true;
			this.blackMap =  new HashMap<String,Set<String>>();
			for(Map.Entry<String,Object> entry : blackMap.entrySet()) {
				this.blackMap.put(
					entry.getKey(), new HashSet((List<String>)entry.getValue()));
			}
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #whiteMap} parameter.  A whiteMap
	 * contains all the pairs property - list of objects that are meant to be
	 * indexed.
	 *
	 * @param whiteMap - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #whiteMap}
	 * parameter set
	 */
	public Harvester rdfWhiteMap(Map<String,Object> whiteMap) {
		if(whiteMap != null || !whiteMap.isEmpty()) {
			hasWhiteMap = true;
			this.whiteMap =  new HashMap<String,Set<String>>();
			for(Map.Entry<String,Object> entry : whiteMap.entrySet()) {
				this.whiteMap.put(
					entry.getKey(), new HashSet((List<String>)entry.getValue()));
			}
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #uriDescriptionList} parameter.
	 * Whenever {@link #uriDescriptionList} is set, all the objects represented
	 * by URIs are replaced with the resource's label. The label is the first
	 * of the properties in the given list, for which the resource has an object.
	 *
	 * @param uriList - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #uriDescriptionList}
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
	 * Sets the {@link Harvester}'s {@link #uriDescriptionList} parameter.
	 * When it is set to true  a new property is added to each resource:
	 * <http://www.w3.org/1999/02/22-rdf-syntax-ns#about>, having the value
	 * equal to the resource's URI.
	 *
	 * @param rdfAddUriForResource - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #addUriForResource}
	 * parameter set
	 */
	public Harvester rdfAddUriForResource(Boolean rdfAddUriForResource) {
		this.addUriForResource = rdfAddUriForResource;
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #syncConditions} parameter. It
	 * represents the sync query's additional conditions for indexing. These
	 * conditions are added to the time filter.
	 * @param syncCond - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #syncConditions}
	 * parameter set
	 */
	public Harvester rdfSyncConditions(String syncCond) {
		this.syncConditions = syncCond;
		if(!syncCond.isEmpty())
			this.syncConditions += " . ";
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #syncTimeProp} parameter. It
	 * represents the sync query's time parameter used when filtering the
	 * endpoint's last updates.
	 * @param syncTimeProp - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #syncTimeProp}
	 * parameter set
	 */
	public Harvester rdfSyncTimeProp(String syncTimeProp) {
		this.syncTimeProp = syncTimeProp;
		return this;
	}

    /**
     * Sets the {@link Harvester}'s {@link #syncOldData} parameter. When this
     * parameter is set to true, the endpoint will be queried again without the
     * {@link #syncConditions} to update existing resources that were changed.
     * THe default value is true
     * @param syncOldData - a new value for the parameter
     * return the same {@link Harvester} with the {@link #syncOldData}
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

	private void setLastUpdate(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		try {
			bulkRequest.add(client.prepareIndex(indexName, "stats", "1")
					.setSource(jsonBuilder()
							.startObject()
							.field("last_update", sdf.format(date))
							.endObject()));
		} catch (IOException ioe) {
			logger.error("Could not add the stats to ES. {}",
					ioe.getLocalizedMessage());
		}
		bulkRequest.execute().actionGet();
	}


	public void run() {
		long currentTime = System.currentTimeMillis();
		boolean success = false;

		if(indexAll)
			success = runIndexAll();
		else
			success = runSync();

		if (success) {
			setLastUpdate(new Date(currentTime));
		}
	}

	public boolean runSync() {
		logger.info("Starting RDF synchronizer: from [{}], endpoint [{}], "
				    + "index name [{}], type name [{}]",
					startTime, rdfEndpoint,	indexName, typeName);


		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date lastUpdate = new Date(System.currentTimeMillis());


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

		boolean success = sync();

		closed = true;
		logger.info("Ended synchronization from [{}], for endpoint [{}]," +
					"index name [{}], type name [{}] with status {}",
					lastUpdate, rdfEndpoint, indexName, typeName,
					success ? "Success": "Failure");
		return success;
	}

	/**
	 * Get a set of unique queryObjName returned from a select query
	 *
	 * Used to retrieve sets of modified objects used in sync
	 * @param rdfQuery query to execute
	 * @param queryObjName name of the object returned
	 * @return set of values for queryObjectName in the rdfQuery result
	 */
	HashSet<String> executeSyncQuery(String rdfQuery, String queryObjName) {
		HashSet<String> rdfUrls = new HashSet<String>();

		Query query = null;
		try {
			query = QueryFactory.create(rdfQuery);
		} catch (QueryParseException qpe) {
			logger.warn(
					"Could not parse [{}]. Please provide a relevant quey. {}",
					rdfQuery, qpe.getLocalizedMessage());
			return null;
		}

		QueryExecution qexec = QueryExecutionFactory.sparqlService(
				rdfEndpoint, query);
		try {
			ResultSet results = qexec.execSelect();

			while(results.hasNext()) {
				QuerySolution sol = results.nextSolution();
				try {
					String value = sol.getResource(queryObjName).toString();
					rdfUrls.add(value);
				} catch (NoSuchElementException nsee) {
					logger.error(
							"Encountered a NoSuchElementException: "
							+ nsee.getLocalizedMessage());
					return null;
				}
			}
		} catch (Exception e) {
			logger.error(
					"Encountered a [{}] while querying the endpoint for sync",
					e.getLocalizedMessage());
			return null;
		} finally {
			qexec.close();
		}

		return rdfUrls;
	}

	/**
	 * Build a query returning all triples in which members of
	 * uris are the subjects of the triplets.
	 *
	 * If toDescribeURIs is true the query will automatically add logic
	 * to retrieve the labels directly from the SPARQL endpoint.
	 * @param uris URIs for queried resources
	 * @return a CONSTRUCT query string
	 */
	private String getSyncQueryStr(Iterable<String> uris) {
		StringBuilder uriSetStrBuilder = new StringBuilder();
		String delimiter = "";

		uriSetStrBuilder.append("(");
		for (String uri : uris) {
			uriSetStrBuilder.append(delimiter).append(String.format("<%s>", uri));
			delimiter = ", ";
		}
		uriSetStrBuilder.append(")");

		String uriSet = uriSetStrBuilder.toString();

		/* Get base triplets having any element from uris as subject */
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("CONSTRUCT { ?s ?p ?o } WHERE {")
					.append("{?s ?p ?o")
					.append(String.format(" . FILTER (?s in %s )", uriSet));

		/* Perform uri label resolution only if desired */
		if (!toDescribeURIs) {
			queryBuilder.append("}}");
			return queryBuilder.toString();
		}

		/* Filter out properties having a label */
		int index = 0;
		for (String prop: uriDescriptionList) {
			index++;
			String filterTemplate = " . OPTIONAL { ?o <%s> ?o%d } "
								  + " . FILTER(!BOUND(?o%d))";
			queryBuilder.append(String.format(filterTemplate, prop, index, index));
		}
		queryBuilder.append("}");

		/* Add labels for filtered out properties */
		for (String prop : uriDescriptionList) {
			/* Resolve ?o as being the <prop> for resource ?o1 */
			String partQueryTemplate = " UNION "
									 + "{ ?s ?p ?o1"
									 + " . FILTER (?s in %s)"
									 + " . ?o1 <%s> ?o }";
			queryBuilder.append(String.format(partQueryTemplate, uriSet, prop));
		}

		queryBuilder.append("}");
		return queryBuilder.toString();

	}

	/**
	 * Starts a harvester with predefined queries to synchronize with the
	 * changes from the SPARQL endpoint
	 */
	public boolean sync() {
		logger.info("Sync resources newer than {}", startTime);
		String rdfQueryTemplate =
				"PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
				+ "SELECT ?resource WHERE { "
				+ "?resource <%s> ?time . %s"
				+ " FILTER (?time > xsd:dateTime(\"%s\")) }";

		String queryStr = String.format(rdfQueryTemplate, syncTimeProp,
										syncConditions, startTime);
		rdfUrls = executeSyncQuery(queryStr, "resource");

		if (rdfUrls == null) {
			logger.error("Errors occurred during sync procedure. Aborting!");
			return false;
		}

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

		int deleted = 0;
		int count = 0;
		if (this.syncOldData) {
			rdfQueryTemplate = "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
							 + "SELECT ?resource WHERE { "
					   		 + "?resource <%s> ?time ."
					   		 + " FILTER (?time > xsd:dateTime(\"%s\")) }";
			queryStr = String.format(rdfQueryTemplate, syncTimeProp, startTime);

			HashSet<String> notMatchingUrls = executeSyncQuery(queryStr, "resource");

			if (notMatchingUrls == null) {
				logger.error("Errors occurred during modified content sync query. Aborting!");
				return false;
			}

			notMatchingUrls.removeAll(rdfUrls);

			for (String uri : notMatchingUrls) {
				DeleteRequestBuilder request = client.prepareDelete(
											indexName, typeName, uri);
				DeleteResponse response = request.execute().actionGet();

				if (response.isFound()) {
					deleted++;
					logger.info("Deleted resource not matching sync properties: {}", uri);
				}
			}
		}

		/* Prepare a series of bulk uris to be described so we can make
		 * a smaller number of calls to the SPARQL endpoint. */
		ArrayList<ArrayList<String>> bulks = new ArrayList<ArrayList<String>>();
		ArrayList<String> currentBulk = new ArrayList<String>();

		for (String uri : rdfUrls) {
			currentBulk.add(uri);

			if (currentBulk.size() == EEASettings.DEFAULT_BULK_SIZE) {
				bulks.add(currentBulk);
				currentBulk = new ArrayList<String>();
			}
		}

		if (currentBulk.size() > 0) {
			bulks.add(currentBulk);
		}

		/* Execute RDF queries for the resources in each bulk */
		for (ArrayList<String> bulk : bulks) {
			String syncQuery = getSyncQueryStr(bulk);

			try {
				Query query = QueryFactory.create(syncQuery);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(
						rdfEndpoint, query);
				try {
					Model constructModel = ModelFactory.createDefaultModel();
					qexec.execConstruct(constructModel);
					BulkRequestBuilder bulkRequest = client.prepareBulk();

					/**
					 *  When adding the model to ES save old state of toDescribeURIs
					 *  as the query already returned the correct labels.
					 */
					boolean oldToDescribeURIs = toDescribeURIs;
					toDescribeURIs = false;
					addModelToES(constructModel, bulkRequest);
					toDescribeURIs = oldToDescribeURIs;
					count += bulk.size();
				} catch (Exception e) {
					logger.error(
						"Error while querying for modified content. {}",
						e.getLocalizedMessage());
					return false;
				} finally {
					qexec.close();
				}
			} catch (QueryParseException  qpe) {
				logger.warn(
					"Could not parse Sync query. Please provide a relevant query. {}",
					qpe.getLocalizedMessage());
				return false;
			}

		}
		logger.info("Finished synchronisation: Deleted {}, Updated {}/{}",
					deleted, count, rdfUrls.size());
		return true;
	}

	/**
	 * Starts the harvester for queries and/or URLs
	 */
	public boolean runIndexAll() {
		logger.info(
				"Starting RDF harvester: endpoint [{}], queries [{}]," +
				"URLs [{}], index name [{}], typeName [{}]",
 				rdfEndpoint, rdfQueries, rdfUrls, indexName, typeName);

		while (true) {
			if(this.closed){
				logger.info("Ended harvest for endpoint [{}], queries [{}]," +
						"URLs [{}], index name {}, type name {}",
						rdfEndpoint, rdfQueries, rdfUrls, indexName, typeName);
				return true;
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
	 * Query SPARQL endpoint with a CONSTRUCT query
	 * @param qexec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getConstructModel(QueryExecution qexec) {
		return qexec.execConstruct(ModelFactory.createDefaultModel());
	}

	/**
	 * Query SPARQL endpoint with a DESCRIBE query
	 * @param qexec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getDescribeModel(QueryExecution qexec) {
		return qexec.execDescribe(ModelFactory.createDefaultModel());
	}

	/**
	 * Query SPARQL endpoint with a SELECT query
	 * @param qexec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getSelectModel(QueryExecution qexec) {
		Model model = ModelFactory.createDefaultModel();
		Graph graph = model.getGraph();
		ResultSet results = qexec.execSelect();

		while (results.hasNext()) {
			QuerySolution sol = results.next();
			String subject;
			String predicate;
			RDFNode object;

			try {
				subject = sol.getResource("s").toString();
				predicate = sol.getResource("p").toString();
				object = sol.get("o");
			} catch (NoSuchElementException nsee) {
				logger.error("SELECT query does not return a (?s ?p ?o) Triple");
				continue;
			}

			Node objNode;
			if (object.isLiteral()) {
				Literal obj = object.asLiteral();
				objNode = NodeFactory.createLiteral(obj.getString(), obj.getDatatype());
			} else {
				objNode = NodeFactory.createLiteral(object.toString());
			}

			graph.add(new Triple(
					NodeFactory.createURI(subject),
					NodeFactory.createURI(predicate),
					objNode));
		}

		return model;
	}

	/**
	 * Query the SPARQL endpoint with a specified QueryExecution
	 * and return the model
	 * @param qexec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getModel(QueryExecution qexec) {
		switch (rdfQueryType) {
			case CONSTRUCT: return getConstructModel(qexec);
			case DESCRIBE: return getDescribeModel(qexec);
			case SELECT: return getSelectModel(qexec);
		}
		return null;
	}

	/**
	 * Add data to ES given a query execution service
	 * @param qexec query execution service
	 */
	private void harvest(QueryExecution qexec) {
		boolean retry;
		do {
			retry = false;
			try {
				Model model = getModel(qexec);
				addModelToES(model, client.prepareBulk());
			} catch (QueryExceptionHTTP httpe) {
				if (httpe.getResponseCode() >= 500) {
					retry = true;
					logger.error("Encountered an internal server error "
					             + "while harvesting. Retrying!");
				} else {
					throw httpe;
				}
			}
		} while (retry);
	}

	/**
	 * Queries the {@link #rdfEndpoint(String)} with each of the {@link #rdfQueries}
	 * and harvests the results of the query.
	 *
	 * Observation: At this time only the CONSTRUCT and SELECT queries are
	 * supported
	 */
	private void harvestFromEndpoint() {

		Query query = null;
		QueryExecution qexec = null;

		for (String rdfQuery : rdfQueries) {
			logger.info(
				"Harvesting with query: [{}] on index [{}] and type [{}]",
 				rdfQuery, indexName, typeName);

			try {
				query = QueryFactory.create(rdfQuery);
			} catch (QueryParseException qpe) {
				logger.error(
						"Could not parse [{}]. Please provide a relevant query. {}",
						rdfQuery, qpe);
				continue;
			}

			qexec = QueryExecutionFactory.sparqlService(rdfEndpoint, query);

			try {
				harvest(qexec);
			} catch (Exception e) {
				logger.error("Exception [{}] occured while harvesting",
							 e.getLocalizedMessage());
			} finally {
				qexec.close();
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
	 * Get JSON map for a given resource by applying the river settings
	 * @param rs resource being processed
	 * @param properties properties to be indexed
	 * @param model model returned by the indexing query
	 * @return map of properties to be indexed for res
	 */
	private Map<String, ArrayList<String>> getJsonMap(Resource rs, Set<Property> properties, Model model) {
		Map<String, ArrayList<String>> jsonMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> results = new ArrayList<String>();

		if(addUriForResource) {
			results.add("\"" + rs.toString() + "\"");
			jsonMap.put(
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#about",
					results);
		}

		Set<String> rdfLanguages = new HashSet<String>();

		for(Property prop: properties) {
			long t = System.currentTimeMillis();
			NodeIterator niter = model.listObjectsOfProperty(rs,prop);
			String property = prop.toString();
			results = new ArrayList<String>();


			String lang = "";
			String currValue = "";

			while (niter.hasNext()) {
				RDFNode node = niter.next();
				currValue = getStringForResult(node);
				if (addLanguage) {
					try {
						lang = node.asLiteral().getLanguage();
						if (!lang.isEmpty()) {
							rdfLanguages.add("\"" + lang + "\"");
						}
					} catch (Exception e) {
						
					}
				}

				String shortValue = currValue;

				int currlen = currValue.length();
				// Unquote string
				if (currlen > 1)
					shortValue = currValue.substring(1, currlen - 1);

				// If either whiteMap does contains shortValue
				// or blackMap contains the value
				// skip adding it to the index
				boolean whiteMapCond = hasWhiteMap
						&& whiteMap.containsKey(property)
						&& !whiteMap.get(property).contains(shortValue);

				boolean blackMapCond = hasBlackMap
						&& blackMap.containsKey(property)
						&& blackMap.get(property).contains(shortValue);
				if (whiteMapCond || blackMapCond) {
					continue;
				} else {
					if (willNormalizeObj && normalizeObj.containsKey(shortValue)) {
						results.add("\"" + normalizeObj.get(shortValue) + "\"");
					} else {
						results.add(currValue);
					}
				}
			}

			// Do not index empty properties
			if (results.isEmpty()) continue;

			if (willNormalizeProp && normalizeProp.containsKey(property)) {
				property = normalizeProp.get(property);
				if (jsonMap.containsKey(property)) {
					jsonMap.get(property).addAll(results);
				} else {
					jsonMap.put(property, results);
				}
			} else {
				jsonMap.put(property, results);
			}
		}

		if(addLanguage) {
			if(rdfLanguages.isEmpty() && !language.isEmpty())
				rdfLanguages.add(language);
			if(!rdfLanguages.isEmpty())
				jsonMap.put(
						"language", new ArrayList<String>(rdfLanguages));
		}

		if (willNormalizeMissing) {
			for (Map.Entry<String, String> it : normalizeMissing.entrySet()) {
				if (!jsonMap.containsKey(it.getKey())) {
					ArrayList<String> res = new ArrayList<String>();
					res.add("\"" + it.getValue() + "\"");
					jsonMap.put(it.getKey(), res);
				}
			}
		}

		return jsonMap;
	}

	/**
	 * Index all the resources in a Jena Model to ES
	 *
	 * @param model the model to index
	 * @param bulkRequest a BulkRequestBuilder
	 */
	private void addModelToES(Model model, BulkRequestBuilder bulkRequest) {       
		long startTime = System.currentTimeMillis();
		long bulkLength = 0;
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

		while(rsiter.hasNext()) {
			Resource rs = rsiter.nextResource();
			Map<String, ArrayList<String>> jsonMap = getJsonMap(rs, properties, model);

			bulkRequest.add(client.prepareIndex(indexName, typeName, rs.toString())
					.setSource(mapToString(jsonMap)));
			bulkLength++;

			// We want to execute the bulk for every  DEFAULT_BULK_SIZE requests
			if(bulkLength % EEASettings.DEFAULT_BULK_SIZE == 0) {
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				// After executing, flush the BulkRequestBuilder.
				bulkRequest = client.prepareBulk();

				if (bulkResponse.hasFailures()) {
					processBulkResponseFailure(bulkResponse);
				}
			}
		}

		// Execute remaining requests
		if(bulkRequest.numberOfActions() > 0) {
			BulkResponse response = bulkRequest.execute().actionGet();
			// Handle failure by iterating through each bulk response item
			if(response.hasFailures()) {
				processBulkResponseFailure(response);
			}
                     
		}

        // Show time taken to index the documents
		logger.info("Indexed {} documents on {}/{} in {} seconds",
					bulkLength, indexName, typeName,
					(System.currentTimeMillis() - startTime)/ 1000.0);
	}
        
        
	/**
	 *  This method processes failures by iterating through each bulk response item
	 *  @param response, a BulkResponse
	 **/
	private void processBulkResponseFailure(BulkResponse response) {
		logger.warn("There was failures when executing bulk : " + response.buildFailureMessage());

		if(!logger.isDebugEnabled()) return;

		for(BulkItemResponse item: response.getItems()) {
			if (item.isFailed()) {
				logger.debug("Error {} occured on index {}, type {}, id {} for {} operation "
							, item.getFailureMessage(), item.getIndex(), item.getType(), item.getId()
							, item.getOpType());
			}
		}
	}

	/**
	 * Converts a map of results to a String JSON representation for it
	 * @param map a map that matches properties with an ArrayList of
	 * values
	 * @return the JSON representation for the map, as a String
	 */
	private String mapToString(Map<String, ArrayList<String>> map) {
		StringBuilder result = new StringBuilder("{");
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

				if (literalJavaClass.equals(Boolean.class)
					|| Number.class.isAssignableFrom(literalJavaClass)) {

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
		if (uriLabelCache.containsKey(uri)) {
			return uriLabelCache.get(uri);
		}
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
							if(!result.isEmpty()) {
								uriLabelCache.put(uri, result);
								return result;
							}
						}
					} catch(Exception e) {
						keepTrying = true;
						logger.warn("Could not get label for uri {}. Retrying.",
									uri);
					} finally { qexec.close();}
				}
			} catch (QueryParseException qpe) {
				logger.error("Exception for query {}. The label cannot be obtained",
							 innerQuery);
			}
		}
		return uri;
	}
}
