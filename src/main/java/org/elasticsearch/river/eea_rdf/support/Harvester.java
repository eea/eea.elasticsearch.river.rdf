package org.elasticsearch.river.eea_rdf.support;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.client.Client;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFLanguages ;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.graph.GraphMaker;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.NodeFactory;


import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.lang.StringBuffer;
import java.lang.Exception;
import java.lang.ClassCastException;

public class Harvester implements Runnable {

		private final ESLogger logger = Loggers.getLogger(Harvester.class);

		private List<String> rdfUrls;
		private String rdfEndpoint;
		private String rdfQuery;
	  private TimeValue rdfTimeout;

		private Client client;
		private String riverIndexName;
		private String riverName;
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

		public Harvester rdfTimeout(TimeValue timeout) {
				this.rdfTimeout = timeout;
				return this;
		}

		public Harvester client(Client client) {
				this.client = client;
				return this;
		}

		public Harvester riverIndexName(String riverIndexName) {
				this.riverIndexName = riverIndexName;
				return this;
		}

		public Harvester riverName(String riverName) {
				this.riverName = riverName;
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
								delay("Ended harvest", "");
								return;
						}

						/**
							* Harvest from SPARQL endpoint
							*/
						Query query = QueryFactory.create(rdfQuery);
						QueryExecution qexec = QueryExecutionFactory.sparqlService(
									rdfEndpoint,
									query);
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
												String predicate = sol
																					.getResource(iterS.next())
																							.toString();
												String subject = sol
																					.getResource(iterS.next())
																							.toString();
												String object = sol.get(iterS.next()).toString();

												graph.add(new Triple(
																		NodeFactory.createURI(subject),
																		NodeFactory.createURI(predicate),
																		NodeFactory.createLiteral(object)));

										} catch(NoSuchElementException nsee) {
											logger.info("Could not index {} {}: Query result was not a triple",
													sol.toString(), nsee.toString());
										}

										BulkRequestBuilder bulkRequest = client.prepareBulk();
										addModelToES(sparqlModel, bulkRequest);
								}
						} catch(Exception e) {
								logger.info("Exception on endpoint stuff [{}]",
														e.toString());
						} finally { qexec.close();}

						/**
							* Harvest from RDF dumps
							*/

						for(String url:rdfUrls) {

								if(url.isEmpty()) continue;

								logger.info("Harvesting url [{}]", url);


								Model model = ModelFactory.createDefaultModel();
								RDFDataMgr.read(model, url.trim(), RDFLanguages.RDFXML);

								BulkRequestBuilder bulkRequest = client.prepareBulk();

								addModelToES(model, bulkRequest);
								}



						closed = true;
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
							properties.add(st.getPredicate());

					}

					ResIterator rsiter = model.listSubjects();

					while(rsiter.hasNext()){
							Resource rs = rsiter.nextResource();
							StringBuffer json = new StringBuffer();
							json.append("{");

							for(Property prop: properties) {
									NodeIterator niter = model.listObjectsOfProperty(rs,prop);
									if(niter.hasNext()) {
											StringBuffer result = new StringBuffer();
											result.append("[");

											int count = 0;
											String currValue = "";

											while(niter.hasNext()) {
													count++;
													currValue = niter.next()
																	.toString().trim()
																		.replaceAll("\n", " ");
													currValue = currValue.replaceAll("\"", "\'");
													result.append("\"");
													result.append(currValue);
													result.append("\",");
											}

											result.setCharAt(result.length()-1, ']');
											if(count == 1) {
													currValue = "\"" + currValue + "\"";
													result = new StringBuffer(currValue);
											}
											json.append("\"");
											json.append(prop.toString());
											json.append("\" : ");
											json.append(result.toString());
											json.append(",\n");
									}
							}

							json.setCharAt(json.length() - 2, '}');
							bulkRequest.add(client.prepareIndex(indexName, typeName, rs.toString())
										.setSource(json.toString()));
							BulkResponse bulkResponse = bulkRequest.execute().actionGet();

					}
		}


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
