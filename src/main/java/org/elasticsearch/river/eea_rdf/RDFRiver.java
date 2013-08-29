package org.elasticsearch.river.eea_rdf;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.eea_rdf.support.Harvester;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;

public class RDFRiver extends AbstractRiverComponent implements River {
		private final static String DEFAULT_INDEX_NAME = "eeaRDF";
		private final static String DEFAULT_TYPE_NAME = "eeaRDF";
		private final static int DEFAULT_BULK_SIZE = 100;
		private final static int DEFAULT_BULK_REQ = 30;
		private final static String DEFAULT_QUERY = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 10";

		private volatile Harvester harvester;
	  private volatile Thread harvesterThread;

		@Inject
		public RDFRiver(RiverName riverName, RiverSettings settings,
				                    @RiverIndexName String riverIndexName,
														Client client) {

				super(riverName, settings);

				harvester = new Harvester()
								.client(client)
								.riverIndexName(riverIndexName)
								.riverName(riverName.name());

				if (settings.settings().containsKey("eeaRDF")) {
			 				Map<String, Object> eeaSettings = (Map<String, Object>)settings.
													settings().get("eeaRDF");

							harvester
											.rdfUrl(XContentMapValues.nodeStringValue(
														eeaSettings.get("urls"), "[]"))
											.rdfEndpoint(XContentMapValues.nodeStringValue(
														eeaSettings.get("endpoint"), "http://localhost"))
											/*TODO: select all when available*/
											.rdfQuery(XContentMapValues.nodeStringValue(
														eeaSettings.get("query"), DEFAULT_QUERY))
											.rdfTimeout(XContentMapValues.nodeTimeValue(
														eeaSettings.get("timeout"),
														TimeValue.timeValueSeconds(60)));
				}
				else {
						throw new	ElasticSearchIllegalArgumentException(
								"There are no eeaRDF settings in the	river settings");
				}

				if(settings.settings().containsKey("index")){
						Map<String, Object> indexSettings = (Map<String, Object>)settings.
												settings().get("index");
						harvester
										.index(XContentMapValues.nodeStringValue(
													indexSettings.get("index"),
													"rdfdata"))
										.type(XContentMapValues.nodeStringValue(
													indexSettings.get("type"),
													"resource"))
										.maxBulkActions(XContentMapValues.nodeIntegerValue(
													indexSettings.get("bulk_size"),
													100))
										.maxConcurrentRequests(XContentMapValues.nodeIntegerValue(
													indexSettings.get("max_bulk_requests"),
													30));
				}
				else {
						harvester
										.index("rdfdata")
										.type("resource")
										.maxBulkActions(100)
										.maxConcurrentRequests(130);
				}

		}

		@Override
		public void start() {
				harvesterThread =	EsExecutors.daemonThreadFactory(
						settings.globalSettings(),
						"eea_rdf_river(" + riverName().name() +	")")
					.newThread(harvester);
				harvesterThread.start();
		}

		@Override
		public void close() {
				logger.info("closing EEA RDF river {}", riverName.name());
				harvester.setClose(true);
				harvesterThread.interrupt();
		}
}
