package org.elasticsearch.river.eea_rdf;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.app.Harvester;
import org.elasticsearch.app.EEASettings;

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
	public RDFRiver(RiverName riverName,
					RiverSettings settings,
					@RiverIndexName String riverIndexName,
					Client client) {
		super(riverName, settings);
		harvester = new Harvester();
		harvester.client(client).riverName(riverName.name());
		addHarvesterSettings(settings);
	}

	/** Type casting accessors for river settings **/
	@SuppressWarnings("unchecked")
	private static Map<String, Object> extractSettings(RiverSettings settings,
													   String key) {
		return (Map<String, Object>)settings.settings().get(key);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, String> getStrStrMapFromSettings(Map<String, Object> settings,
																String key) {
		return (Map<String, String>)settings.get(key);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> getStrObjMapFromSettings(Map<String, Object> settings,
																String key) {
		return (Map<String, Object>)settings.get(key);
	}

	@SuppressWarnings("unchecked")
	private static List<String> getStrListFromSettings(Map<String, Object> settings,
													   String key) {
		return (List<String>)settings.get(key);
	}

	private void addHarvesterSettings(RiverSettings settings) {
		if (!settings.settings().containsKey("eeaRDF")) {
			throw new IllegalArgumentException(
					"There are no eeaRDF settings in the river settings");
		}

		Map<String, Object> rdfSettings = extractSettings(settings, "eeaRDF");

		harvester.rdfIndexType(XContentMapValues.nodeStringValue(
				rdfSettings.get("indexType"), "full"))
				.rdfStartTime(XContentMapValues.nodeStringValue(
						rdfSettings.get("startTime"),""))
				.rdfUris(XContentMapValues.nodeStringValue(
						rdfSettings.get("uris"), "[]"))
				.rdfEndpoint(XContentMapValues.nodeStringValue(
						rdfSettings.get("endpoint"),
						EEASettings.DEFAULT_ENDPOINT))
				.rdfClusterId(XContentMapValues.nodeStringValue(
						rdfSettings.get("cluster_id"),
						EEASettings.DEFAULT_CLUSTER_ID))
				.rdfQueryType(XContentMapValues.nodeStringValue(
						rdfSettings.get("queryType"),
						EEASettings.DEFAULT_QUERYTYPE))
				.rdfListType(XContentMapValues.nodeStringValue(
						rdfSettings.get("listtype"),
						EEASettings.DEFAULT_LIST_TYPE))
				.rdfAddLanguage(XContentMapValues.nodeBooleanValue(
						rdfSettings.get("addLanguage"),
						EEASettings.DEFAULT_ADD_LANGUAGE))
				.rdfAddCounting(XContentMapValues.nodeBooleanValue(
						rdfSettings.get("addCounting"),
						EEASettings.DEFAULT_ADD_COUNTING))
				.rdfLanguage(XContentMapValues.nodeStringValue(
						rdfSettings.get("language"),
						EEASettings.DEFAULT_LANGUAGE))
				.rdfAddUriForResource(XContentMapValues.nodeBooleanValue(
						rdfSettings.get("includeResourceURI"),
						EEASettings.DEFAULT_ADD_URI))
				.rdfURIDescription(XContentMapValues.nodeStringValue(
						rdfSettings.get("uriDescription"),
						EEASettings.DEFAULT_URI_DESCRIPTION))
				.rdfSyncConditions(XContentMapValues.nodeStringValue(
						rdfSettings.get("syncConditions"),
						EEASettings.DEFAULT_SYNC_COND))
				.rdfGraphSyncConditions(XContentMapValues.nodeStringValue(
						rdfSettings.get("graphSyncConditions"), ""))
				.rdfSyncTimeProp(XContentMapValues.nodeStringValue(
						rdfSettings.get("syncTimeProp"),
						EEASettings.DEFAULT_SYNC_TIME_PROP))
				.rdfSyncOldData(XContentMapValues.nodeBooleanValue(
						rdfSettings.get("syncOldData"),
						EEASettings.DEFAULT_SYNC_OLD_DATA));

		if (rdfSettings.containsKey("proplist")) {
			harvester.rdfPropList(getStrListFromSettings(rdfSettings, "proplist"));
		}
		if(rdfSettings.containsKey("query")) {
			harvester.rdfQuery(getStrListFromSettings(rdfSettings, "query"));
		} else {
			harvester.rdfQuery(EEASettings.DEFAULT_QUERIES);
		}

		if(rdfSettings.containsKey("normProp")) {
			harvester.rdfNormalizationProp(getStrObjMapFromSettings(rdfSettings, "normProp"));
		}
		if(rdfSettings.containsKey("normMissing")) {
			harvester.rdfNormalizationMissing(getStrObjMapFromSettings(rdfSettings, "normMissing"));
		}
		if(rdfSettings.containsKey("normObj")) {
			harvester.rdfNormalizationObj(getStrStrMapFromSettings(rdfSettings, "normObj"));
		}
		if(rdfSettings.containsKey("blackMap")) {
			harvester.rdfBlackMap(getStrObjMapFromSettings(rdfSettings, "blackMap"));
		}
		if(rdfSettings.containsKey("whiteMap")) {
			harvester.rdfWhiteMap(getStrObjMapFromSettings(rdfSettings, "whiteMap"));
		}

		if(settings.settings().containsKey("index")){
			Map<String, Object> indexSettings = extractSettings(settings, "index");
			harvester.index(XContentMapValues.nodeStringValue(
							indexSettings.get("index"),
							EEASettings.DEFAULT_INDEX_NAME))
					 .type(XContentMapValues.nodeStringValue(
							indexSettings.get("type"),
							EEASettings.DEFAULT_TYPE_NAME));
		}
		else {
			harvester.index(EEASettings.DEFAULT_INDEX_NAME)
					 .type(EEASettings.DEFAULT_TYPE_NAME);
		}
	}
	
	public void start() {
		harvesterThread = EsExecutors.daemonThreadFactory(
				settings.globalSettings(),
				"eea_rdf_river(" + riverName().name() +	")")
			.newThread(harvester);
		harvesterThread.start();
	}

	public void close() {
		harvester.log("Closing EEA RDF river [" + riverName.name() + "]");
		harvester.close();
		harvesterThread.interrupt();
	}
}
