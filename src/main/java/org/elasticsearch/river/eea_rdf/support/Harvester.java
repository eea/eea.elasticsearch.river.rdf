package org.elasticsearch.river.eea_rdf.support;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.client.Client;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;

import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFLanguages ;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.graph.GraphMaker;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.datatypes.RDFDatatype;

import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.lang.StringBuffer;
import java.lang.Exception;
import java.lang.Integer;
import java.lang.Byte;
import java.lang.ClassCastException;

public class Harvester implements Runnable {

	private final ESLogger logger = Loggers.getLogger(Harvester.class);

	private List<String> rdfUrls;
	private String rdfEndpoint;
	private String rdfQuery;
	private int rdfQueryType;
	private List<String> rdfPropList;
	private Boolean rdfListType = false;
	private Boolean hasList = false;
	private Map<String, String> normalizationMap;
	private Boolean willNormalize = false;
	private Boolean addLanguage = false;
	private List<String> uriDescriptionList;
	private Boolean toDescribeURIs = false;

	private Client client;
	private String indexName;
	private String typeName;
	private int maxBulkActions;
	private int maxConcurrentRequests;

	private Boolean closed = false;

	public Harvester rdfUrl(String url) {
		url = url.substring(1, url.length() - 1);
		rdfUrls = Arrays.asList(url.split(","));
		return this;
	}

	public Harvester rdfEndpoint(String endpoint) {
		this.rdfEndpoint = endpoint;
		return this;
	}

	public Harvester rdfQuery(String query) {
		this.rdfQuery = query;
		return this;
	}

	public Harvester rdfQueryType(String queryType) {
		if(queryType.equals("select"))
			this.rdfQueryType = 1;
		else
			this.rdfQueryType = 0;
		return this;
	}

	public Harvester rdfPropList(String list) {
		list = list.substring(1, list.length() - 1);
		rdfPropList = Arrays.asList(list.split(","));
		if(!list.isEmpty())
			hasList = true;
		return this;
	}

	public Harvester rdfListType(String listType) {
		if(listType.equals("white"))
			this.rdfListType = true;
		return this;
	}

	public Harvester rdfLanguage(Boolean rdfLanguage) {
		addLanguage = rdfLanguage;
		return this;
	}

	public Harvester rdfNormalizationMap(Map<String, String> normalizationMap) {
		if(normalizationMap != null || !normalizationMap.isEmpty()) {
			willNormalize = true;
			this.normalizationMap = normalizationMap;
		}
		return this;
	}

	public Harvester rdfURIDescription(String uriList) {
		uriList = uriList.substring(1, uriList.length() - 1);
		if(!uriList.isEmpty())
			toDescribeURIs = true;
		uriDescriptionList = Arrays.asList(uriList.split(","));
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

	public Harvester maxBulkActions(int maxBulkActions) {
		this.maxBulkActions = maxBulkActions;
		return this;
	}

	public Harvester maxConcurrentRequests(int maxConcurrentRequests) {
		this.maxConcurrentRequests = maxConcurrentRequests;
		return this;
	}

	public void setClose(Boolean value) {
		this.closed = value;
	}

	@Override
	public void run() {

		logger.info(
				"Starting RDF harvester: endpoint [{}], query [{}]," +
				"URLs [{}], index name [{}], typeName {}",
				rdfEndpoint, rdfQuery, rdfUrls, indexName, typeName);

		while (true) {
			if(this.closed){
				logger.info("Ended harvest for endpoint [{}], query [{}]," +
						"URLs [{}], index name {}, type name {}",
						rdfEndpoint, rdfQuery, rdfUrls, indexName, typeName);
				return;
			}

			/**
			 * Harvest from a SPARQL endpoint
			 */
			if(!rdfQuery.isEmpty()) {
				harvestFromEndpoint();
			}

			/**
			 * Harvest from RDF dumps
			 */
			harvestFromDumps();

			closed = true;
		}
	}

	private void harvestFromEndpoint() {
		try {
			Query query = QueryFactory.create(rdfQuery);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(
					rdfEndpoint,
					query);
			if(rdfQueryType == 1) {
				try {
					ResultSet results = qexec.execSelect();
					Model sparqlModel = ModelFactory.createDefaultModel();

					Graph graph = sparqlModel.getGraph();

					while(results.hasNext()) {
						QuerySolution sol = results.nextSolution();
						Iterator<String> iterS = sol.varNames();

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
							logger.info("Could not index [{}] / {}: Query result was" +
									"not a triple",	sol.toString(), nsee.toString());
						}

						BulkRequestBuilder bulkRequest = client.prepareBulk();
						addModelToES(sparqlModel, bulkRequest);
					}
				} catch(Exception e) {
					logger.info("Encountered a [{}] when quering the endpoint", e.toString());
				} finally { qexec.close();}
			}
			else{
				try{
					Model constructModel = ModelFactory.createDefaultModel();
					qexec.execConstruct(constructModel);

					BulkRequestBuilder bulkRequest = client.prepareBulk();
					addModelToES(constructModel, bulkRequest);

				} catch (Exception e) {
					logger.info("Could not index due to [{}]", e.toString());
				} finally {qexec.close();}
			}
		} catch (QueryParseException qpe) {
			logger.info(
					"Could not parse [{}]. Please provide a relevant query",
					rdfQuery);
		}
	}

	private void harvestFromDumps() {
		for(String url:rdfUrls) {
			if(url.isEmpty()) continue;

			logger.info("Harvesting url [{}]", url);

			Model model = ModelFactory.createDefaultModel();
			RDFDataMgr.read(model, url.trim(), RDFLanguages.RDFXML);
			BulkRequestBuilder bulkRequest = client.prepareBulk();

			addModelToES(model, bulkRequest);
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
			if(!hasList
					|| (rdfListType && rdfPropList.contains(prop.toString()))
					|| (!rdfListType && !rdfPropList.contains(prop.toString()))) {
				properties.add(prop);
			}
		}

		ResIterator rsiter = model.listSubjects();

		while(rsiter.hasNext()){

			Resource rs = rsiter.nextResource();
			Map<String, ArrayList<String>> jsonMap = new HashMap<String,
				ArrayList<String>>();
			Set<String> rdfLanguages = new HashSet<String>();

			for(Property prop: properties) {
				NodeIterator niter = model.listObjectsOfProperty(rs,prop);
				if(niter.hasNext()) {
					ArrayList<String> results = new ArrayList<String>();
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
							} catch (Exception e){}
						}
						results.add(currValue);
					}

					String property, value;

					if(willNormalize && normalizationMap.containsKey(prop.toString())) {
						property = normalizationMap.get(prop.toString());
						if(jsonMap.containsKey(property)) {
							results.addAll(jsonMap.get(property));
							jsonMap.put(property, results);
						} else {
							jsonMap.put(property, results);
						}
					} else {
						property = prop.toString();
						jsonMap.put(property,results);
					}
				}
			}
			if(addLanguage && rdfLanguages.size() > 0) {
				jsonMap.put("language", new ArrayList<String>(rdfLanguages));
			}

			bulkRequest.add(client.prepareIndex(indexName, typeName, rs.toString())
				.setSource(mapToString(jsonMap)));
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		}
	}

	private String mapToString(Map<String, ArrayList<String>> map) {
		StringBuffer result = new StringBuffer("{");
		for(Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
			ArrayList<String> value = entry.getValue();
			if(value.size() == 1)
				result.append("\"" + entry.getKey() + "\" : " + value.get(0) + ",\n");
			else
				result.append("\"" + entry.getKey() + "\" : " + value.toString() + ",\n");
		}
		result.setCharAt(result.length() - 2, '}');
		return result.toString();
	}

	private String getStringForResult(RDFNode node) {
		String result = "";
		boolean quote = false;

		if(node.isLiteral()) {
			Object literalValue = node.asLiteral().getValue();
			try {
				Class literalJavaClass = node.asLiteral()
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
				String label = getLabelForUri(result);
				if(!label.isEmpty()) {
					result = label;
				}
			}
			quote = true;
		}
		if(quote) {
			result = "\"" + result + "\"";
		}
		return result;
	}


  private String getLabelForUri(String uri) {
		String result = "";
		for(String prop:uriDescriptionList) {
			String innerQuery = "SELECT ?r WHERE {<" + result+"> <" +
				prop + "> ?r } LIMIT 1";

			Query query = QueryFactory.create(innerQuery);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(
					rdfEndpoint,
					query);
			try {
				ResultSet results = qexec.execSelect();
				Model sparqlModel = ModelFactory.createDefaultModel();

				if(results.hasNext()) {
					QuerySolution sol = results.nextSolution();
	        result = sol.getResource("r").toString();
					if(!result.isEmpty())
						return result;
				}
			} catch(Exception e){
			}finally { qexec.close();}
		}
		return result;
	}

	@Deprecated
	private void delay(String reason, String url) {
		int time = 1000;
		if(!url.isEmpty()) {
			logger.info("Info: {}, waiting for url [{}] ", reason, url);
		}
		else {
			logger.info("Info: {}", reason);
			time = 2000;
		}

		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {}
	}
}
