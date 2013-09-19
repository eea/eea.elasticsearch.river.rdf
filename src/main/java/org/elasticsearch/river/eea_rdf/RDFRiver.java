package org.elasticsearch.river.eea_rdf;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.eea_rdf.support.Harvester;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;

public class RDFRiver extends AbstractRiverComponent implements River {
	private volatile Harvester harvester;
	private volatile Thread harvesterThread;

	@Inject
	public RDFRiver(RiverName riverName, RiverSettings settings,
			@RiverIndexName String riverIndexName, Client client) {

		super(riverName, settings);

		harvester = new Harvester().client(client);

		if (settings.settings().containsKey("eeaRDF")) {
			Map<String, Object> eeaSettings = (Map<String, Object>)settings.
													settings().get("eeaRDF");

			harvester
				.rdfUrl(XContentMapValues.nodeStringValue(
							eeaSettings.get("uris"), "[]"))
				.rdfEndpoint(XContentMapValues.nodeStringValue(
							eeaSettings.get("endpoint"),
							EEASettings.DEFAULT_ENDPOINT))
				.rdfQuery(XContentMapValues.nodeStringValue(
							eeaSettings.get("query"),
							EEASettings.DEFAULT_QUERY))
				.rdfQueryType(XContentMapValues.nodeStringValue(
							eeaSettings.get("queryType"),
							EEASettings.DEFAULT_QUERYTYPE))
				.rdfPropList(XContentMapValues.nodeStringValue(
							eeaSettings.get("proplist"),
							EEASettings.DEFAULT_PROPLIST))
				.rdfListType(XContentMapValues.nodeStringValue(
							eeaSettings.get("listtype"),
							EEASettings.DEFAULT_LIST_TYPE))
				.rdfLanguage(XContentMapValues.nodeBooleanValue(
							eeaSettings.get("language"),
							EEASettings.DEFAULT_ADD_LANGUAGE))
				.rdfURIDescription(XContentMapValues.nodeStringValue(
							eeaSettings.get("uriDescription"),
							EEASettings.DEFAULT_URI_DESCRIPTION));

			if(eeaSettings.containsKey("normMap")) {
				harvester
					.rdfNormalizationMap((
								Map<String,String>)eeaSettings.get("normMap"));
			}
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
							"resource"));
		}
		else {
			harvester
				.index(EEASettings.DEFAULT_INDEX_NAME)
				.type(EEASettings.DEFAULT_TYPE_NAME);
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
