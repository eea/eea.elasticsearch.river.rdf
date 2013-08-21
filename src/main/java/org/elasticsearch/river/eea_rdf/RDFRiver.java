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
														eeaSettings.get("url"), "http://localhost"))
											.rdfPoll(XContentMapValues.nodeTimeValue(
														eeaSettings.get("poll"),
														TimeValue.timeValueMinutes(60)))
											.rdfSet(XContentMapValues.nodeStringValue(
														eeaSettings.get("set"), null))
											.rdfFrom(XContentMapValues.nodeStringValue(
														eeaSettings.get("from"),	null))
											.rdfUntil(XContentMapValues.nodeStringValue(
														eeaSettings.get("until"),	null))
											.rdfTimeout(XContentMapValues.nodeTimeValue(
														eeaSettings.get("timeout"),
														TimeValue.timeValueSeconds(60)));
				}
				else {
						throw new	ElasticSearchIllegalArgumentException(
								"There are no eeaRDF settings in the	river settings");

						/*
						 * TODO: Support other indexes
						 */
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
