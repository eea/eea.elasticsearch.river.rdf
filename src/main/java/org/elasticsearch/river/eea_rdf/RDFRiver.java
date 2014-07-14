package org.elasticsearch.river.eea_rdf;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
import java.util.List;

/**
 *
 * @author iulia
 *
 */
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
				.rdfIndexType(XContentMapValues.nodeStringValue(
							eeaSettings.get("indexType"), "full"))
				.rdfStartTime(XContentMapValues.nodeStringValue(
							eeaSettings.get("startTime"),""))
				.rdfUrl(XContentMapValues.nodeStringValue(
							eeaSettings.get("uris"), "[]"))
				.rdfEndpoint(XContentMapValues.nodeStringValue(
							eeaSettings.get("endpoint"),
							EEASettings.DEFAULT_ENDPOINT))
				.rdfQueryType(XContentMapValues.nodeStringValue(
							eeaSettings.get("queryType"),
							EEASettings.DEFAULT_QUERYTYPE))
				.rdfListType(XContentMapValues.nodeStringValue(
							eeaSettings.get("listtype"),
							EEASettings.DEFAULT_LIST_TYPE))
				.rdfAddLanguage(XContentMapValues.nodeBooleanValue(
							eeaSettings.get("addLanguage"),
							EEASettings.DEFAULT_ADD_LANGUAGE))
				.rdfLanguage(XContentMapValues.nodeStringValue(
							eeaSettings.get("language"),
							EEASettings.DEFAULT_LANGUAGE))
				.rdfAddUriForResource(XContentMapValues.nodeBooleanValue(
							eeaSettings.get("includeResourceURI"),
							EEASettings.DEFAULT_ADD_URI))
				.rdfURIDescription(XContentMapValues.nodeStringValue(
							eeaSettings.get("uriDescription"),
							EEASettings.DEFAULT_URI_DESCRIPTION))
				.rdfSyncConditions(XContentMapValues.nodeStringValue(
							eeaSettings.get("syncConditions"),
							EEASettings.DEFAULT_SYNC_COND))
				.rdfSyncTimeProp(XContentMapValues.nodeStringValue(
							eeaSettings.get("syncTimeProp"),
							EEASettings.DEFAULT_SYNC_TIME_PROP));

			if(eeaSettings.containsKey("proplist")) {
				harvester.rdfPropList((
							List<String>)eeaSettings.get("proplist"));
			}
			if(eeaSettings.containsKey("query")) {
				harvester.rdfQuery((
							List<String>)eeaSettings.get("query"));
			} else {
				harvester.rdfQuery(EEASettings.DEFAULT_QUERIES);
			}
			if(eeaSettings.containsKey("normProp")) {
				harvester
					.rdfNormalizationProp((
								Map<String,String>)eeaSettings.get("normProp"));
			}
			if(eeaSettings.containsKey("normObj")) {
				harvester
					.rdfNormalizationObj((
								Map<String,String>)eeaSettings.get("normObj"));
			}
			if(eeaSettings.containsKey("blackMap")) {
				harvester
					.rdfBlackMap((
								Map<String,Object>)eeaSettings.get("blackMap"));
			}
			if(eeaSettings.containsKey("whiteMap")) {
				harvester
					.rdfWhiteMap((
								Map<String,Object>)eeaSettings.get("whiteMap"));
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
		harvesterThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(),
				"eea_rdf_river(" + riverName().name() +	")")
			.newThread(harvester);
		harvesterThread.start();
	}

	@Override
	public void close() {
		harvester.log("Closing EEA RDF river {}" + riverName.name());
		harvester.setClose(true);
		harvesterThread.interrupt();
	}
}
