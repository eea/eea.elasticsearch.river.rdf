package  org.elasticsearch.app;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.tdb.store.Hash;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.app.logging.ESLogger;

import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.lang.NullPointerException;

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

	//TODO: LOGGER
    private final ESLogger logger = Loggers.getLogger(Harvester.class);

	private String rdfEndpoint = "";

	private String rdfClusterId = "";

	/* Harvester operation info */
	private Boolean indexAll = true;
	private String startTime = "";

	/* Harvest from uris options */
	private Set<String> rdfUris = new HashSet<String>();

	/* Harvest from query options */
	private List<String> rdfQueries = new ArrayList<String>();
	private QueryType rdfQueryType;

	/* WhiteList / BlackList properties */
	private List<String> rdfPropList = new ArrayList<String>();
	private Boolean isWhitePropList = false;

	/* Normalization options */
	private Map<String, Object> normalizeProp = new HashMap<String, Object>();
	private Map<String, String> normalizeObj = new HashMap<String, String>();
	private Map<String, Object> normalizeMissing = new HashMap<String, Object>();

	/* Language options */
	private Boolean addLanguage = false;
	private String language;

        /* Counting options */
        private Boolean addCounting = false;

	/* Resource augmenting options */
	private List<String> uriDescriptionList = new ArrayList<String>();
	private Boolean addUriForResource = false;

	/* BlackMap and WhiteMap */
	private Map<String,Set<String>> blackMap = new HashMap<String, Set<String>>();
	private Map<String,Set<String>> whiteMap = new HashMap<String, Set<String>>();

	/* Sync options */
	private String syncConditions;
	private String syncTimeProp;
	private String graphSyncConditions;
	private Boolean syncOldData;

	/* ES api options */
	private RestHighLevelClient client;
	private String indexName;
	private String typeName;
	private String riverName;
	private String riverIndex;

	private Boolean closed = false;

	public void log(String message) { logger.info(message); }

	public void close() { closed = true; }

	private HashMap<String, String> uriLabelCache = new HashMap<String, String>();

	public String getRiverName(){
		return this.riverName;
	}

	/**
 	 * Sets the {@link Harvester}'s {@link #rdfUris} parameter
 	 * @param url - a list of urls
 	 * @return the {@link Harvester} with the {@link #rdfUris} parameter set
 	 */
	public Harvester rdfUris(String url) {
		url = url.substring(1, url.length() - 1);
		rdfUris = new HashSet<String>(Arrays.asList(url.split(",")));
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

	public Harvester rdfClusterId(String clusterId) {
		rdfClusterId = clusterId;
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfQueries} parameter
	 * @param query - new list of queries
	 * @return the same {@link Harvester} with the {@link #rdfQueries} parameter
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
			//TODO: LOG
			//logger.info("Bad query type: {}", queryType);



			/* River process can't continue */
			throw e;
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #rdfPropList} parameter
	 * @param list - a list of properties names that are either required in
	 * the object description, or undesired, depending on its
	 * {@link #isWhitePropList}
	 * @return the same {@link Harvester} with the {@link #rdfPropList}
	 * parameter set
	 */
	public Harvester rdfPropList(List<String> list) {
		if(!list.isEmpty()) {
			rdfPropList = new ArrayList<String>(list);
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #isWhitePropList} parameter
	 * @param listType - a type ("black" or "white") for the {@link #rdfPropList}
	 * in case it exists
	 * @return the same {@link Harvester} with the {@link #isWhitePropList}
	 * parameter set
	 * @Observation A blacklist contains properties that should not be indexed
	 * with the data while a white-list contains all the properties that should
	 * be indexed with the data.
	 */
	public Harvester rdfListType(String listType) {
		if (listType.equals("white"))
			isWhitePropList = true;
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

        public Harvester rdfAddCounting(Boolean rdfAddCounting) {
                addCounting = rdfAddCounting;
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
	 */
	public Harvester rdfNormalizationProp(Map<String, Object> normalizeProp) {
		if (normalizeProp != null && !normalizeProp.isEmpty()) {
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
	 */
	public Harvester rdfNormalizationObj(Map<String, String> normalizeObj) {
		if(normalizeObj != null && !normalizeObj.isEmpty()) {
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
	 */
	public Harvester rdfNormalizationMissing(Map<String, Object> normalizeMissing) {
		if(normalizeMissing != null && !normalizeMissing.isEmpty()) {
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
	@SuppressWarnings("unchecked")
	public Harvester rdfBlackMap(Map<String,Object> blackMap) {
		if(blackMap != null && !blackMap.isEmpty()) {
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
	@SuppressWarnings("unchecked")
	public Harvester rdfWhiteMap(Map<String,Object> whiteMap) {
		if(whiteMap != null && !whiteMap.isEmpty()) {
			this.whiteMap = new HashMap<String,Set<String>>();
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
	 */
	public Harvester rdfURIDescription(String uriList) {
		uriList = uriList.substring(1, uriList.length() - 1);
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
	 * conditions are added within the graphs matching the time filter.
	 * Use the ?resource variable to address the resource that should match
	 * the conditions within the graph.
	 * @param syncCond - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #syncConditions}
	 * parameter set
	 */
	public Harvester rdfSyncConditions(String syncCond) {
		this.syncConditions = syncCond;
		if (!syncCond.isEmpty() &&
			!syncCond.trim().endsWith(".")) {
			this.syncConditions += " . ";
		}
		return this;
	}

	/**
	 * Sets the {@link Harvester}'s {@link #graphSyncConditions} parameter. It
	 * represents the sync query's graph conditions for indexing. These
	 * conditions are added to the graphs matching the time filter.
	 * Use the ?graph variable to address the graph that should match
	 * the conditions. In this context, ?resource is also bound.
	 * @param graphSyncConditions - a new value for the parameter
	 * @return the same {@link Harvester} with the {@link #syncConditions}
	 * parameter set
	 */
	public Harvester rdfGraphSyncConditions(String graphSyncConditions) {
		this.graphSyncConditions = graphSyncConditions;
		if (!graphSyncConditions.isEmpty() &&
		    !graphSyncConditions.trim().endsWith(".")) {
			this.graphSyncConditions += " . ";
		}
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

	public Harvester client(RestHighLevelClient client) {
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

	public Harvester riverName(String riverName) {
		this.riverName = riverName;
		return this;
	}

	public Harvester riverIndex(String riverIndex){
		this.riverIndex = riverIndex;
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


	private void setLastUpdate(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		BulkRequest bulkRequest = new BulkRequest();

		//TODO: prepareBulk, setSource - DONE
		/*BulkRequestBuilder bulkRequest = client.prepareBulk();
		try {
			bulkRequest.add(new IndexRequest(indexName, "stats", "1")
					.setSource(jsonBuilder()
							.startObject()
							.field("last_update", sdf.format(date))
							.endObject()));
		} catch (IOException ioe) {
			logger.error("Could not add the stats to ES. {}",
					ioe.getLocalizedMessage());
		}

		bulkRequest.execute().actionGet();*/

        try {
            //TODO: why Bulk?
            bulkRequest.add(new IndexRequest(indexName, "stats", "1")
                    .source(jsonBuilder()
                        .startObject()
                            .field("last_update", sdf.format(date))
                            .endObject()));
        } catch (IOException ioe) {
            logger.error("Could not add the stats to ES. {}",
                    ioe.getLocalizedMessage());
        }

        try {
            //TODO: async
            client.bulk(bulkRequest);
        } catch (IOException ex) {
            logger.error("Could not do bulk request {}",
                    ex.getLocalizedMessage());
        }

    }

	private String getLastUpdate() {
		String res = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

		/*try {
            GetResponse response = client
				.prepareGet(indexName, "stats", "1")
				.setFields("last_update")
				.execute()
				.actionGet();
		    if (!response.getFields().isEmpty()) {
			    res = (String) response.getField("last_update").getValue();
		    } else {
			    res = sdf.format(new Date(0));
		    }
		} catch ( ElasticsearchIllegalStateException ex) {

			logger.error("Could not get last_update, use Date(0)",
					ex.());
			res = sdf.format(new Date(0));
		} catch (NullPointerException ex) {
            logger.error("Could not get last_update, use Date(0)",
					ex.getLocalizedMessage());
			res = sdf.format(new Date(0));
		}*/

		//TODO : WIP
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName + "_status");



		/*SearchRequest searchRequest = new SearchRequest(indexName + "_status");
		searchRequest.types("last_update");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchRequest.scroll(TimeValue.timeValueMinutes(1L));

		ArrayList<Long> updates = new ArrayList<Long>();

		try {
			SearchResponse searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			String scrollId = searchResponse.getScrollId();
			SearchHits hits = searchResponse.getHits();

			SearchHit[] searchHits = hits.getHits();
			for (SearchHit hit : searchHits) {
				// do something with the SearchHit
				if(hit.hasSource()){
					Map<String, Object> sourceAsMap = hit.getSourceAsMap();
					Long updated_at = (Long) sourceAsMap.get("updated_at");
					updates.add(updated_at);
				}
			}
			if(updates.size() > 0){
				res = Collections.max(updates).toString();
			} else {
				res = sdf.format(new Date(0));
			}

		} catch (IOException e) {
			logger.error("Could not get last_update, use Date(0)",
					e);
			res = sdf.format(new Date(0));
		}*/

        /*try {
            //TODO: move to async
            GetResponse response = client.get(getRequest);
            if(!response.getFields().isEmpty()){
                res = response.getField("last_update").getValue();
            } else {
                res = sdf.format(new Date(0));
            }
        } catch (ElasticsearchException e) {
            logger.error("Could not get last_update, use Date(0)",
                    e);
            res = sdf.format(new Date(0));
        } catch (NullPointerException ex){
            logger.error("Could not get last_update, use Date(0)",
                    ex.getLocalizedMessage());
            res = sdf.format(new Date(0));
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        return res;
	}


	public void run() {
		long currentTime = System.currentTimeMillis();
		boolean success;

		if (startTime.isEmpty()) startTime = getLastUpdate();

		if (indexAll)
			success = runIndexAll();
		else
			success = runSync();

		if (success) {
			setLastUpdate(new Date(currentTime));
		}

		//TODO: admin
		/*client.admin().indices()
			  .prepareDeleteMapping("_river").setType(riverName)
			  .execute();*/

		/* Any code after this step would not be executed as
		   the master will interrupt the harvester thread after
		   deleting the _river.
		 */
	}

	public boolean runSync() {
		//TODO: LOG
		/*logger.info("Starting RDF synchronization: endpoint [{}], " +
				    "index name [{}], type name [{}]",
					rdfEndpoint, indexName, typeName);*/

		boolean success = sync();
		closed = true;

		/*logger.info("Ended synchronization from [{}], for endpoint [{}]," +
					"index name [{}], type name [{}] with status {}",
					startTime, rdfEndpoint, indexName, typeName,
					success ? "Success": "Failure");*/
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
                logger.info("Start executeSyncQuery");
                logger.info("QUERY:");
                logger.info(rdfQuery);
		HashSet<String> rdfUrls = new HashSet<String>();

		Query query;
		try {
			query = QueryFactory.create(rdfQuery);
		} catch (QueryParseException qpe) {
			logger.warn(
					"Could not parse [{}]. Please provide a relevant query. {}",
					rdfQuery, qpe.getLocalizedMessage());
			return null;
		}

		QueryExecution qExec = QueryExecutionFactory.sparqlService(
				rdfEndpoint, query);
		try {
			ResultSet results = qExec.execSelect();

			while(results.hasNext()) {
				QuerySolution sol = results.nextSolution();
				try {
					String value = sol.getResource(queryObjName).toString();
					rdfUrls.add(value);
				} catch (NoSuchElementException e) {
					//TODO: LOG
					logger.error(
							"Encountered a NoSuchElementException: "
							+ e.getLocalizedMessage());
					return null;
				}
			}
		} catch (Exception e) {
			//TODO: LOG
			logger.error(
					"Encountered a [{}] while querying the endpoint for sync",
					e.getLocalizedMessage());
			return null;
		} finally {
			qExec.close();
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
		if (uriDescriptionList.isEmpty()) {
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

		/* We need this redundant clause as UNION queries can't handle sub-selects
		 * without a prior clause.
		 */
		String redundantClause = "<http://www.w3.org/2000/01/rdf-schema#Class> "
							   + "a <http://www.w3.org/2000/01/rdf-schema#Class>";

		/* Add labels for filtered out properties */
		for (String prop : uriDescriptionList) {
			/* Resolve ?o as str(?label) for the resource ?res
			 * label is taken as being ?res <prop> ?label
			 *
			 * We need to take str(?label) in order to drop
			 * language references of the terms so that the document
			 * is indexed with a language present only in it's top-level
			 * properties.
			 *
			 * As some Virtuoso versions do not allow the usage
			 * of BIND so we have to create a sub-select in order to bind
			 * ?o to str(?label)
			 *
			 * The sub-select works only with a prior clause.
			 * We are using a redundant clause that is always true
			 */
			String partQueryTemplate = " UNION "
									 + "{ "
									 + redundantClause + " . "
									 + "{ SELECT ?s ?p (str(?label) as ?o) { "
									 + "   ?s ?p ?res"
									 + "   . FILTER (?s in %s)"
									 + "   . ?res <%s> ?label }}}";
			queryBuilder.append(String.format(partQueryTemplate, uriSet, prop));
		}

		queryBuilder.append("}");
		return queryBuilder.toString();

	}


	/**
	 * Remove the documents from ElasticSearch that are not present in
	 * uris
	 * @param uris uris that should be present in the index.
	 * @return true if the action completed, false if it failed during
	 * the process.
	 */
	private int removeMissingUris(Set<String> uris, String clusterId) {
		int searchKeepAlive = 60000;
		int count = 0;

		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(indexName).types(typeName);

		searchRequest.scroll(scroll);

		SearchResponse searchResponse = null;
		try {
			searchResponse = client.search(searchRequest);
			String scrollId = searchResponse.getScrollId();
			SearchHit[] searchHits = searchResponse.getHits().getHits();

			//process the hits
			//noinspection Duplicates
			for(SearchHit hit : searchHits){
				String hitClusterId = (String)hit.getSourceAsMap().getOrDefault("cluster_id", clusterId);

				if (hitClusterId != clusterId){
					continue;
				}
				if (uris.contains(hit.getId()))
					continue;

				DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, hit.getId());
				DeleteResponse deleteResponse = client.delete(deleteRequest);

				ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
				if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

				}
				if (shardInfo.getFailed() > 0) {
					for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
						String reason = failure.reason();
						logger.warn("Deletion failure: " + reason);
					}
				}
				if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
					count++;
				}
				//if (deleteResponse.isFound()) count++;

			}

			while (searchHits != null && searchHits.length > 0) {
				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				searchResponse = client.searchScroll(scrollRequest);
				scrollId = searchResponse.getScrollId();
				searchHits = searchResponse.getHits().getHits();

				//process the hits
				//noinspection Duplicates
				for(SearchHit hit : searchHits){
					String hitClusterId = (String)hit.getSourceAsMap().getOrDefault("cluster_id", clusterId);

					if (hitClusterId != clusterId){
						continue;
					}
					if (uris.contains(hit.getId()))
						continue;

					DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, hit.getId());
					DeleteResponse deleteResponse = client.delete(deleteRequest);

					ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
					if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

					}
					if (shardInfo.getFailed() > 0) {
						for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
							String reason = failure.reason();
							logger.warn("Deletion failure: " + reason);
						}
					}
					if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
						count++;
					}
					//if (deleteResponse.isFound()) count++;

				}
			}

			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
			boolean succeeded = clearScrollResponse.isSucceeded();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ElasticsearchStatusException es){
			logger.warn("Index not found: [{}]", es.getMessage() );
		} catch (ElasticsearchException exception) {
			if (exception.status() == RestStatus.CONFLICT) {
				logger.warn("There was a conflict: [{}]", exception.getMessage());
			} else {
				exception.printStackTrace();
			}
		}
		//TODO: prepareSearch, prepareDelete, prepareSearchScroll
		/*SearchResponse response = client.prepareSearch()
				.setIndices(indexName)
				.setTypes(typeName)
				.setScroll(new TimeValue(searchKeepAlive))
				.setQuery(QueryBuilders.matchAllQuery())
				.execute()
				.actionGet();
		while (response.getHits().getHits().length > 0) {
			for (SearchHit hit : response.getHits()) {

                String hitClusterId = (String)hit.getSource().getOrDefault("cluster_id", clusterId);
                if (hitClusterId != clusterId){
                    continue;
                }
				if (uris.contains(hit.getId()))
					continue;

				DeleteResponse deleteResponse =
						client.prepareDelete(indexName, typeName, hit.getId())
								.execute()
								.actionGet();
				if (deleteResponse.isFound()) count++;
			}

			response = client.prepareSearchScroll(response.getScrollId())
					.setScroll(new TimeValue(searchKeepAlive))
					.execute()
					.actionGet();
		}*/
		return count;
	}

	/**
	 * Starts a harvester with predefined queries to synchronize with the
	 * changes from the SPARQL endpoint
	 */
	public boolean sync() {
		//TODO: LOG
		/*logger.info("Sync resources newer than {}", startTime);*/

		String rdfQueryTemplate =
				"PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
				+ "SELECT DISTINCT ?resource WHERE { "
				+ " GRAPH ?graph { %s }"
				+ " ?graph <%s> ?time .  %s "
				+ " FILTER (?time > xsd:dateTime(\"%s\")) }";

		String queryStr = String.format(rdfQueryTemplate, syncConditions,
										syncTimeProp, graphSyncConditions,
										startTime);
		Set<String> syncUris = executeSyncQuery(queryStr, "resource");

		if (syncUris == null) {
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
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			queryStr = String.format(rdfQueryTemplate, syncConditions,
									 syncTimeProp, graphSyncConditions,
									 sdf.format(new Date(0)));

			HashSet<String> allIndexURIs = executeSyncQuery(queryStr, "resource");

			if (allIndexURIs == null) {
				logger.error("Errors occurred during modified content sync query. Aborting!");
				return false;
			}
			deleted = removeMissingUris(allIndexURIs, this.rdfClusterId);
		}

		/* Prepare a series of bulk uris to be described so we can make
		 * a smaller number of calls to the SPARQL endpoint. */
		ArrayList<ArrayList<String>> bulks = new ArrayList<ArrayList<String>>();
		ArrayList<String> currentBulk = new ArrayList<String>();

		for (String uri : syncUris) {
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
		ArrayList<ArrayList<String>> bulksWithErrors = new ArrayList<ArrayList<String>>();
		boolean isBulkWithErrors = false;
		ArrayList<String> urisWithErrors = new ArrayList<String>();
		ArrayList<String> urisUpdatedWithSuccess = new ArrayList<String>();
		while (true){
			for (ArrayList<String> bulk : bulks) {
				String syncQuery = getSyncQueryStr(bulk);
				logger.info("QUERY:");
				logger.info(syncQuery);

				try {
					Query query = QueryFactory.create(syncQuery);
					QueryExecution qExec = QueryExecutionFactory.sparqlService(
								rdfEndpoint, query);
					try {
						Model constructModel = ModelFactory.createDefaultModel();
						qExec.execConstruct(constructModel);

						//TODO: prepareBulk
						/* BulkRequestBuilder bulkRequest = client.prepareBulk();*/
						BulkRequest bulkRequest = new BulkRequest();

						/**
						*  When adding the model to ES do not use toDescribeURIs
						*  as the query already returned the correct labels.
						*/

						addModelToES(constructModel, bulkRequest, false);

						count += bulk.size();

						for (String uri : bulk){
							urisUpdatedWithSuccess.add(uri);
						}

					} catch (Exception e) {
						//TODO: LOG
						logger.error("Error while querying for modified content. {}", e.getLocalizedMessage());
						e.printStackTrace();

							if (! isBulkWithErrors){
								for (String uri : bulk){
									ArrayList<String> currentErrorBulk = new ArrayList<String>();
									currentErrorBulk.add(uri);
									bulksWithErrors.add(currentErrorBulk);
								}
							} else{
								for (String uri : bulk){
									urisWithErrors.add(String.format("%s %s", uri, e.getLocalizedMessage()));
								}
							}

							if (e.getMessage() == "Future got interrupted"){
								return false;
							}
						} finally {
							qExec.close();
						}
				} catch (QueryParseException  qpe) {
					//TODO: LOG
					logger.warn("Could not parse Sync query. Please provide a relevant query. {}", qpe.getLocalizedMessage());
					if (! isBulkWithErrors){
						for (String uri : bulk){
							ArrayList<String> currentErrorBulk = new ArrayList<String>();
							currentErrorBulk.add(uri);
							bulksWithErrors.add(currentErrorBulk);
						}
					} else {
						for (String uri : bulk){
							ArrayList<String> currentErrorBulk = new ArrayList<String>();
							urisWithErrors.add(String.format("%s %s", uri, qpe.getLocalizedMessage()));
						}
					}
					return false;
				}

			}

			if (bulksWithErrors.size() == 0){
				break;
			}

			if (isBulkWithErrors){
				break;
			}

			logger.warn("There were bulks with errors. Try again each resource one by one.");
			logger.warn("Resources with possible errors:");
			for (ArrayList<String> bulk : bulksWithErrors){
				for (String uri : bulk) {
					logger.warn(uri);
				}
			}

			isBulkWithErrors = true;
			bulks = bulksWithErrors;
		}

		//TODO: LOG
		logger.info("Finished synchronisation: Deleted {}, Updated {}/{}",
				deleted, count, syncUris.size());

		logger.info("Uris updated with success:");

		for (String uri : urisUpdatedWithSuccess) {
			logger.info(uri);
		}

        if (urisWithErrors.size() > 0){
		//TODO: LOG
        logger.error("There were {} uris with errors:", urisWithErrors.size());
			for (String uri : urisWithErrors) {
				logger.error(uri);
			}
		}
		return true;
	}

	/**
	 * Starts the harvester for queries and/or URLs
	 */
	public boolean runIndexAll() {
		//TODO: LOG
		logger.info(
				"Starting RDF harvester: endpoint [{}], queries [{}]," +
				"URIs [{}], index name [{}], typeName [{}]",
 				rdfEndpoint, rdfQueries, rdfUris, indexName, typeName);

		while (true) {
			if (this.closed){
				//TODO: LOG
				logger.info("Ended harvest for endpoint [{}], queries [{}]," +
							"URIs [{}], index name {}, type name {}",
							rdfEndpoint, rdfQueries, rdfUris, indexName, typeName);
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
	 * @param qExec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getConstructModel(QueryExecution qExec) {
		return qExec.execConstruct(ModelFactory.createDefaultModel());
	}

	/**
	 * Query SPARQL endpoint with a DESCRIBE query
	 * @param qExec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getDescribeModel(QueryExecution qExec) {
		return qExec.execDescribe(ModelFactory.createDefaultModel());
	}

	/**
	 * Query SPARQL endpoint with a SELECT query
	 * @param qExec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getSelectModel(QueryExecution qExec) {
		Model model = ModelFactory.createDefaultModel();
		Graph graph = model.getGraph();
		ResultSet results = qExec.execSelect();

		while (results.hasNext()) {
			QuerySolution sol = results.next();
			String subject;
			String predicate;
			RDFNode object;

			try {
				subject = sol.getResource("s").toString();
				predicate = sol.getResource("p").toString();
				object = sol.get("o");
			} catch (NoSuchElementException e) {
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
	 * @param qExec QueryExecution encapsulating the query
	 * @return model retrieved by querying the endpoint
	 */
	private Model getModel(QueryExecution qExec) {
		switch (rdfQueryType) {
			case CONSTRUCT: return getConstructModel(qExec);
			case DESCRIBE: return getDescribeModel(qExec);
			case SELECT: return getSelectModel(qExec);
		}
		return null;
	}

	/**
	 * Add data to ES given a query execution service
	 * @param qExec query execution service
	 */
	private void harvest(QueryExecution qExec) {
		boolean retry;
		do {
			retry = false;
			try {
				Model model = getModel(qExec);
				//TODO: prepareBulk
				/*addModelToES(model, client.prepareBulk(), true);*/
			} catch (QueryExceptionHTTP e) {
				if (e.getResponseCode() >= 500) {
					retry = true;
					logger.error("Encountered an internal server error "
					             + "while harvesting. Retrying!");
				} else {
					throw e;
				}
			}
		} while (retry);
	}

	/**
	 * Queries the {@link #rdfEndpoint(String)} with each of the {@link #rdfQueries}
	 * and harvests the results of the query.
	 */
	private void harvestFromEndpoint() {

                logger.info("harvestFromEndpoint");
		Query query;
		QueryExecution qExec;

		for (String rdfQuery : rdfQueries) {
			if (closed) break;

			//TODO:LOG
			/*logger.info(
				"Harvesting with query: [{}] on index [{}] and type [{}]",
 				rdfQuery, indexName, typeName);*/

			try {
                                logger.info("QUERY:");
                                logger.info(rdfQuery);
				query = QueryFactory.create(rdfQuery);
			} catch (QueryParseException qpe) {
				//TODO:LOG
				/*logger.error(
						"Could not parse [{}]. Please provide a relevant query. {}",
						rdfQuery, qpe);*/
				continue;
			}

			qExec = QueryExecutionFactory.sparqlService(rdfEndpoint, query);

			try {
				harvest(qExec);
			} catch (Exception e) {
				//TODO:LOG
				/*logger.error("Exception [{}] occurred while harvesting",
							 e.getLocalizedMessage());*/
			} finally {
				qExec.close();
			}
		}
	}

	/**
	 * Harvests all the triplets from each URI in the @rdfUris list
	 */
	private void harvestFromDumps() {
		for(String uri : rdfUris) {
			if(uri.isEmpty()) continue;

			//TODO:LOG
			/*logger.info("Harvesting uri [{}]", uri);*/

			Model model = ModelFactory.createDefaultModel();
			try {
				RDFDataMgr.read(model, uri.trim(), RDFLanguages.RDFXML);
				//TODO: prepareBulk
				/*BulkRequestBuilder bulkRequest = client.prepareBulk();
				addModelToES(model, bulkRequest, true);*/
			}
			catch (RiotException re) {
                //TODO:LOG
				/*logger.error("Illegal xml character [{}]", re.getLocalizedMessage());*/

			}
			catch (Exception e) {
                //TODO:LOG
				/*logger.error("Exception when harvesting url: {}. Details: {}",
					uri, e.getLocalizedMessage());*/
			}
		}
	}

    private Map<String, ArrayList<String>> addCountingToJsonMap(Map<String, ArrayList<String>> jsonMap){
        Iterator it = jsonMap.entrySet().iterator();
        Map<String, ArrayList<String>> countingMap = new HashMap<String, ArrayList<String>>();
        ArrayList<String> itemsCount = new ArrayList<String>();
        while (it.hasNext()) {
            itemsCount = new ArrayList<String>();
            Map.Entry<String, ArrayList<String>> pair = (Map.Entry<String, ArrayList<String>>)it.next();
            itemsCount.add(Integer.toString(pair.getValue().size()));
            countingMap.put("items_count_" + pair.getKey(), itemsCount);
        }
        jsonMap.putAll(countingMap);
        return jsonMap;
    }
	/**
	 * Get JSON map for a given resource by applying the river settings
	 * @param rs resource being processed
	 * @param properties properties to be indexed
	 * @param model model returned by the indexing query
	 * @param getPropLabel if set to true all URI property values will be indexed
	 *                     as their label. The label is taken as the value of
	 *                     one of the properties set in {@link #uriDescriptionList}.
	 * @return map of properties to be indexed for res
	 */
	private Map<String, ArrayList<String>> getJsonMap(Resource rs, Set<Property> properties, Model model,
													  boolean getPropLabel) {
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
			NodeIterator niter = model.listObjectsOfProperty(rs,prop);
			String property = prop.toString();
			results = new ArrayList<String>();


			String currValue;

			while (niter.hasNext()) {
				RDFNode node = niter.next();
				currValue = getStringForResult(node, getPropLabel);

				String shortValue = currValue;

				int currLen = currValue.length();
				// Unquote string
				if (currLen > 1)
					shortValue = currValue.substring(1, currLen - 1);

				// If either whiteMap does contains shortValue
				// or blackMap contains the value
				// skip adding it to the index
				boolean whiteMapCond = whiteMap.containsKey(property)
						&& !whiteMap.get(property).contains(shortValue);
				boolean blackMapCond = blackMap.containsKey(property)
						&& blackMap.get(property).contains(shortValue);

				if (whiteMapCond || blackMapCond) {
					continue;
				}
				if (normalizeObj.containsKey(shortValue)) {
                                        if (!results.contains("\"" + normalizeObj.get(shortValue) + "\"")){
					    results.add("\"" + normalizeObj.get(shortValue) + "\"");
                                        }
				} else {
                                        if (!results.contains(currValue)){
					    results.add(currValue);
                                        }
				}
			}

			// Do not index empty properties
			if (results.isEmpty()) continue;

			if (normalizeProp.containsKey(property)) {
				Object norm_property = normalizeProp.get(property);
                                if (norm_property instanceof String){
                                    property = norm_property.toString();
				    if (jsonMap.containsKey(property)) {
					    jsonMap.get(property).addAll(results);
				    } else {
					    jsonMap.put(property, results);
				    }
                                }
                                else {
                                    if (norm_property instanceof List<?>){
                                        for (String norm_prop : ((List<String>) norm_property)) {
                                            if (jsonMap.containsKey(norm_prop)) {
                                                jsonMap.get(norm_prop).addAll(results);
                                            } else {
                                                jsonMap.put(norm_prop, results);
                                            }
                                        }
                                    }
                                }
			} else {
				jsonMap.put(property, results);
			}
		}

		if(addLanguage) {
                    HashSet<Property> allProperties = new HashSet<Property>();

                    StmtIterator it = model.listStatements();
                    while(it.hasNext()) {
                        Statement st = it.nextStatement();
                        Property prop = st.getPredicate();

                        allProperties.add(prop);
                    }


                    for(Property prop: allProperties) {
                        String property = prop.toString();
                        NodeIterator niter = model.listObjectsOfProperty(rs,prop);
                        String lang;

                        while (niter.hasNext()) {
                            RDFNode node = niter.next();
                            if (addLanguage) {
                                if (node.isLiteral()) {
                                    lang = node.asLiteral().getLanguage();
                                    if (!lang.isEmpty()) {
                                        rdfLanguages.add("\"" + lang + "\"");
                                    }
                                }
                            }
                        }
                    }

		    if(rdfLanguages.isEmpty() && !language.isEmpty())
		        rdfLanguages.add(language);
			if(!rdfLanguages.isEmpty())
			    jsonMap.put(
			        "language", new ArrayList<String>(rdfLanguages));
	        }

		for (Map.Entry<String, Object> it : normalizeMissing.entrySet()) {
			if (!jsonMap.containsKey(it.getKey())) {
				ArrayList<String> res = new ArrayList<String>();
                                Object miss_values = it.getValue();
                                if (miss_values instanceof String){
				    res.add("\"" + (String)miss_values + "\"");
                                }
                                else {
                                    if (miss_values instanceof List<?>){
                                        for (String miss_value: ((List<String>)miss_values)){
                                            res.add("\"" + miss_value + "\"");
                                        }
                                    }
                                }
				jsonMap.put(it.getKey(), res);
			}
		}

		return jsonMap;
	}

	/**
	 * Index all the resources in a Jena Model to ES
	 *
	 * @param model the model to index
	 * @param bulkRequest a BulkRequestBuilder
	 * @param getPropLabel if set to true all URI property values will be indexed
	 *                     as their label. The label is taken as the value of
	 *                     one of the properties set in {@link #uriDescriptionList}.
	 */
	private void addModelToES(Model model, BulkRequest bulkRequest, boolean getPropLabel) {
		long startTime = System.currentTimeMillis();
		long bulkLength = 0;
		HashSet<Property> properties = new HashSet<Property>();
            
		StmtIterator it = model.listStatements();
		while(it.hasNext()) {
			Statement st = it.nextStatement();
			Property prop = st.getPredicate();
			String property = prop.toString();

			if (rdfPropList.isEmpty()
				|| (isWhitePropList && rdfPropList.contains(property))
				|| (!isWhitePropList && !rdfPropList.contains(property))
				|| (normalizeProp.containsKey(property))) {
				properties.add(prop);
			}
		}

		ResIterator resIt = model.listSubjects();

		while(resIt.hasNext()) {
			Resource rs = resIt.nextResource();
			Map<String, ArrayList<String>> jsonMap = getJsonMap(rs, properties, model, getPropLabel);
			if (addCounting){
				jsonMap = addCountingToJsonMap(jsonMap);
			}

			//TODO: prepareIndex - DONE
			bulkRequest.add( new IndexRequest(indexName, typeName, rs.toString())
					//.source(mapToString(jsonMap)));
					.source(jsonMap));

			bulkLength++;

			// We want to execute the bulk for every  DEFAULT_BULK_SIZE requests
			if(bulkLength % EEASettings.DEFAULT_BULK_SIZE == 0) {
				BulkResponse bulkResponse = null;

				//TODO: make request async
				try {
					bulkResponse = client.bulk(bulkRequest);
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (bulkResponse.hasFailures()) {
					processBulkResponseFailure(bulkResponse);
				}

				// After executing, flush the BulkRequestBuilder.
				//TODO: prepareBulk
				/*bulkRequest = client.prepareBulk();*/
				bulkRequest = new BulkRequest();
			}
		}

		// Execute remaining requests
		if(bulkRequest.numberOfActions() > 0) {
			//BulkResponse response = bulkRequest.execute().actionGet();
			BulkResponse response = null;
			try {
				response = client.bulk(bulkRequest);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Handle failure by iterating through each bulk response item
			if(response.hasFailures()) {
				processBulkResponseFailure(response);
			}
		}

        // Show time taken to index the documents
        //TODO:LOG
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
                //TODO:LOG
				logger.debug("Error {} occurred on index {}, type {}, id {} for {} operation "
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
				result.append(String.format("\"%s\" : %s,\n",
						entry.getKey(), value.get(0)));
			else
				result.append(String.format("\"%s\" : %s,\n",
						entry.getKey(), value.toString()));
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
	private String getStringForResult(RDFNode node, boolean getNodeLabel) {
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
			if(getNodeLabel) {
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
		String result;

		if (uriLabelCache.containsKey(uri)) {
			return uriLabelCache.get(uri);
		}

		for(String prop:uriDescriptionList) {
			String innerQuery = "SELECT ?r WHERE {<" + uri + "> <" +
				prop + "> ?r } LIMIT 1";

			try {
				Query query = QueryFactory.create(innerQuery);
				QueryExecution qExec = QueryExecutionFactory.sparqlService(
						rdfEndpoint,
						query);
				boolean keepTrying = true;
				while(keepTrying) {
					keepTrying = false;
					try {
						ResultSet results = qExec.execSelect();

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
                        //TODO:LOG
						/*logger.warn("Could not get label for uri {}. Retrying.",
									uri);*/
					} finally { qExec.close();}
				}
			} catch (QueryParseException qpe) {
                //TODO:LOG
				/*logger.error("Exception for query {}. The label cannot be obtained",
							 innerQuery);*/
			}
		}
		return uri;
	}
}