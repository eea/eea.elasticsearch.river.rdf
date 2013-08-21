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

import java.util.HashSet;
import java.lang.StringBuffer;

public class Harvester implements Runnable {

		private final ESLogger logger = Loggers.getLogger(Harvester.class);

		private String rdfUrl;
		private TimeValue rdfPoll;
		private String rdfSet;
		private String rdfFrom;
		private String rdfUntil;
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
				this.rdfUrl = url;
				return this;
		}

		public Harvester rdfPoll(TimeValue poll) {
				this.rdfPoll = poll;
				return this;
		}

		public Harvester rdfSet(String set) {
				this.rdfSet = set;
				return this;
		}

		public Harvester rdfFrom(String from) {
				this.rdfFrom = from;
				return this;
		}

		public  Harvester rdfUntil(String until) {
				this.rdfUntil = until;
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
						"starting RDF harvester: URL [{}], set [{}]," +
						" from [{}], until [{}], indexing to [{}]/[{}]," +
						"poll [{}]", rdfUrl, rdfSet, rdfFrom, rdfUntil,
						indexName, typeName, rdfPoll);


				while (true) {
						if(this.closed){
								delay("Ended harvest");
								return;
						}
						Model model = ModelFactory.createDefaultModel();
						RDFDataMgr.read(model, rdfUrl, RDFLanguages.RDFXML);

						BulkRequestBuilder bulkRequest = client.prepareBulk();

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
												result.append("[\"");

												int count = 0;
												String currValue = "";

												while(niter.hasNext()) {
														count++;
														currValue = niter.next().toString().trim();

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
												json.append(",");
										}
								}

								json.setCharAt(json.length() - 1, '}');

								bulkRequest.add(client.prepareIndex("eeardf", "eeardf", rs.toString())
																	.setSource(json.toString()));
								BulkResponse bulkResponse = bulkRequest.execute().actionGet();

						}

						closed = true;
				}
		}

		private void delay(String reason) {
				if (rdfPoll.millis() > 0L) {
						logger.info(
								"Info: {}, waiting {} for url [{}] set [{}]",
								reason, rdfPoll, rdfUrl, rdfSet);
				}
				try {
						Thread.sleep(rdfPoll.millis());
				} catch (InterruptedException e) {}

		}


}
