package org.elasticsearch.river.eea_rdf.settings;

public abstract class EEASettings {

	public final static String DEFAULT_INDEX_NAME = "rdfdata";
	public final static String DEFAULT_TYPE_NAME = "resource";
	public final static int DEFAULT_BULK_SIZE = 100;
	public final static int DEFAULT_BULK_REQ = 30;
	public final static String DEFAULT_QUERY = "";
	public final static String DEFAULT_ENDPOINT =	"http://semantic.eea.europa.eu/sparql";
	public final static String DEFAULT_QUERYTYPE = "select";
	public final static String DEFAULT_PROPLIST = "[]";
	public final static String DEFAULT_LIST_TYPE = "white";
	public final static Boolean DEFAULT_ADD_LANGUAGE = true;
	public final static String DEFAULT_URI_DESCRIPTION =
		"[\"http://www.w3.org/2000/01/rdf-schema#label\", "
		+ "http://purl.org/dc/terms/title\"]";

	public static String parseForJson(String text) {
		return text.trim().replaceAll("[\n\r]", " ")
			.replace('\"', '\'')
			.replace("\t", "    ")
			.replace("\\'", "\'")
			.replaceAll("\\\\x[a-fA-F0-9][a-fA-F0-9]", "_");
	}
}
