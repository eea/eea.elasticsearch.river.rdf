package org.elasticsearch.river.eea_rdf.settings;

public class EEASettings {

	public final static String DEFAULT_INDEX_NAME = "rdfdata";
	public final static String DEFAULT_TYPE_NAME = "resource";
	public final static int DEFAULT_BULK_SIZE = 100;
	public final static int DEFAULT_BULK_REQ = 30;
	public final static String DEFAULT_QUERY = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 10";
	public final static String DEFAULT_ENDPOINT =	"http://semantic.eea.europa.eu/sparql";
	public final static String DEFAULT_QUERYTYPE = "select";

	public EEASettings(){
	}

	public static String parseForJson(String text) {
			return text.trim().replaceAll("[\n\r]", " ")
								.replace('\"', '\'')
								.replace("\t", "    ")
								.replace("\\'", "\'")
								.replaceAll("\\\\x[a-f0-9][a-f_0-9]", "_");

	}
}
