package org.elasticsearch.app.support;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import org.elasticsearch.app.EEASettings;
import org.elasticsearch.app.Harvester;
import org.elasticsearch.app.debug.JSONMap;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;

import java.util.*;

class Pair<X, Y> {
    public final X value;
    public final Y language;

    public Pair(X value, Y language) {
        this.value = value;
        this.language = language;
    }

    public Y getLanguage() {
        return language;
    }

    public X getValue() {
        return value;
    }
}

public class ESNormalizer {
    private Harvester harvester;
    private Resource rs;
    private Set<Property> properties;
    private Model model;
    private boolean getPropLabel;

    private HashMap<String, HashMap<String, Object>> jsonMaps = new HashMap<>();

    private boolean addUriForResource;
    private Map<String, Object> normalizeProp;
    private Map<String, Set<String>> whiteMap;
    private Map<String, Set<String>> blackMap;
    private Map<String, String> normalizeObj;
    private Boolean addLanguage;
    private String language;

    private final ESLogger logger = Loggers.getLogger(ESNormalizer.class);

    private Set<String> rdfLanguages = new HashSet<>();
    private Map<String, Object> normalizeMissing;


    public void setLanguage(String language) {
        this.language = language;
    }

    public void setNormalizeMissing(Map<String, Object> normalizeMissing) {
        this.normalizeMissing = normalizeMissing;
    }


    public void setAddUriForResource(boolean addUriForResource) {
        this.addUriForResource = addUriForResource;
    }

    public void setNormalizeProp(Map<String, Object> normalizeProp) {
        this.normalizeProp = normalizeProp;
    }

    public void setWhiteMap(Map<String, Set<String>> whiteMap) {
        this.whiteMap = whiteMap;
    }

    public void setBlackMap(Map<String, Set<String>> blackMap) {
        this.blackMap = blackMap;
    }

    public void setNormalizeObj(Map<String, String> normalizeObj) {
        this.normalizeObj = normalizeObj;
    }

    public void setAddLanguage(Boolean addLanguage) {
        this.addLanguage = addLanguage;
    }

    public ESNormalizer() {
        jsonMaps.put("", new JSONMap());
    }

    public ESNormalizer(Resource rs, Set<Property> properties, Model model, boolean getPropLabel, Harvester harvester) {
        this();
        this.rs = rs;
        this.properties = properties;
        this.model = model;
        this.getPropLabel = getPropLabel;
        this.harvester = harvester;
    }

    public void process() {
        //todo: return multiple terms
        addUriForResource();

        for (Property prop : properties) {
            processProperty(prop);
        }

        normalizeMissing();

        if (jsonMaps.keySet().size() > 1)
            addSharedPropertiesToLanguages();
    }

    private void addSharedPropertiesToLanguages() {
        HashMap<String, Object> sharedPropertiesJson = jsonMaps.get("");
        for (String lang : jsonMaps.keySet()) {
            if (lang.equals("")) continue;
            HashMap<String, Object> languageJson = jsonMaps.get(lang);
            for (String prop : sharedPropertiesJson.keySet()) {
                ArrayList<Object> temp = (ArrayList<Object>) sharedPropertiesJson.get(prop);
                if (languageJson.containsKey(prop))
                    temp.addAll((Collection<?>) languageJson.get(prop));
                languageJson.put(prop, temp);
            }
        }
        jsonMaps.remove("");
    }

    private void normalizeMissing() {
        for (Map.Entry<String, Object> it : normalizeMissing.entrySet()) {
            if (!jsonMaps.get("").containsKey(it.getKey())) {
                ArrayList<Object> res = new ArrayList<>();
                Object miss_values = it.getValue();

                if (miss_values instanceof String) {
                    res.add((String) miss_values);
                } else {
                    if (miss_values instanceof List<?>) {

                        for (String miss_value : ((List<String>) miss_values)) {
                            res.add(miss_value);
                        }
                    } else if (miss_values instanceof Number) {
                        res.add(miss_values);
                    } else {
                        res.add(miss_values.toString());
                    }
                }
//                if (res.size() == 1)
//                    jsonMaps.get("").put(it.getKey(), res.get(0));
//                else
                jsonMaps.get("").put(it.getKey(), res);
            }
        }
    }

    private void processProperty(Property prop) {

        NodeIterator niter = model.listObjectsOfProperty(rs, prop);
        String property = prop.toString();
        //todo: optimize
        ArrayList<Object> results;
        String lang;

        Pair<String, String> currValue;
        //hasWorkflowState
        while (niter.hasNext()) {
            results = new ArrayList<>();
            RDFNode node = niter.next();
            currValue = getStringForResult(node, getPropLabel);
            //todo: here
            lang = currValue.getLanguage();
            rdfLanguages.add(lang);
            if (!jsonMaps.keySet().contains(lang))
                jsonMaps.put(lang, new JSONMap());
            if (!lang.equals(""))
                jsonMaps.get(lang).put("language", lang);

            String shortValue = currValue.getValue();

            int currLen = currValue.getValue().length();
            // Unquote string
            /*if (currLen > 1)
                shortValue = currValue.substring(1, currLen - 1);*/

            // If either whiteMap does contains shortValue
            // or blackMap contains the value
            // skip adding it to the index
            boolean whiteMapCond = whiteMap.containsKey(property)
                    && !whiteMap.get(property).contains(shortValue);
            boolean blackMapCond = blackMap.containsKey(property)
                    && blackMap.get(property).contains(shortValue);

            if (whiteMapCond || blackMapCond) {
                continue;
            }
            if (normalizeObj.containsKey(shortValue)) {
                if (!results.contains(normalizeObj.get(shortValue))) {
                    results.add(normalizeObj.get(shortValue));
                }
            } else {
                if (!results.contains(currValue.getValue())) {
                    results.add(currValue.getValue());
                }
            }


            // Do not index empty properties
            if (!results.isEmpty()) {

                if (normalizeProp.containsKey(property)) {
                    Object norm_property = normalizeProp.get(property);
                    if (norm_property instanceof String) {
                        property = norm_property.toString();

                        if (jsonMaps.get(lang).containsKey(property)) {
                            Object temp = jsonMaps.get(lang).get(property);
                            if (temp instanceof List) {
                                results.addAll((List) temp);
//TODO: commented results.get(0)
//                                if (results.size() == 1)
//                                    jsonMaps.get(lang).put(property, results.get(0));
//                                else
                                jsonMaps.get(lang).put(property, results);
                            } else {
                                jsonMaps.get(lang).put(property, results);
                            }
                        } else {
//                            if (results.size() == 1)
//                                jsonMaps.get(lang).put(property, results.get(0));
//                            else
                            jsonMaps.get(lang).put(property, results);
                        }
                    } else {

                        if (norm_property instanceof List<?>) {

                            for (String norm_prop : ((List<String>) norm_property)) {

                                //TODO:
                                if (jsonMaps.get(lang).containsKey(norm_prop)) {
                                    Object temp = jsonMaps.get(lang).get(norm_prop);
                                    if (temp instanceof List) {
                                        results.addAll((List) temp);
//                                        if (results.size() == 1)
//                                            jsonMaps.get(lang).put(norm_prop, results.get(0));
//                                        else
                                        jsonMaps.get(lang).put(norm_prop, results);
                                    } else {
                                        results.add(temp);
                                        jsonMaps.get(lang).put(norm_prop, results);
                                    }
                                } else {
//                                    if (results.size() == 1)
//                                        jsonMaps.get(lang).put(norm_prop, results.get(0));
//                                    else
                                    jsonMaps.get(lang).put(norm_prop, results);
                                }
                            }

                        } else {
                            property = norm_property.toString();

                            if (jsonMaps.get(lang).containsKey(property)) {
                                Object temp = jsonMaps.get(lang).get(property);
                                //TODO:
                                if (temp instanceof List) {
                                    //((List) temp).addAll(results);
//                                    if (results.size() == 1)
//                                        jsonMaps.get(lang).put(property, results.get(0));
//                                    else
                                    jsonMaps.get(lang).put(property, results.toArray());
                                } else {
//                                    if (results.size() == 1)
//                                        jsonMaps.get(lang).put(property, results.get(0));
//                                    else
                                    jsonMaps.get(lang).put(property, results);

                                }
                                //jsonMaps.get(property).addAll(results);
                            } else {
//                                if (results.size() == 1)
//                                    jsonMaps.get(lang).put(property, results.get(0));
//                                else
                                jsonMaps.get(lang).put(property, results);

                            }
                            logger.error("Normalizer error:", norm_property);
                        }
                    }
                } else {
                    if (jsonMaps.get(lang).containsKey(property))
                        results.addAll((Collection<?>) jsonMaps.get(lang).get(property));
                    jsonMaps.get(lang).put(property, results);
                }
            }

        }
    }

    private void addUriForResource() {
        ArrayList<Object> results = new ArrayList<Object>();
        if (addUriForResource) {
            results.add(rs.toString());
//            if (results.size() == 1)
//                jsonMaps.get("").put("about", results.get(0));
//            else
            jsonMaps.get("").put("about", results);

        }
    }


    public HashMap<String, HashMap<String, Object>> getJsonMaps() {
        return jsonMaps;
    }

    /**
     * Builds a String result for Elastic Search from an RDFNode
     *
     * @param node An RDFNode representing the value of a property for a given
     *             resource
     * @return If the RDFNode has a Literal value, among Boolean, Byte, Double,
     * Float, Integer Long, Short, this value is returned, converted to String
     * <p>If the RDFNode has a String Literal value, this value will be
     * returned, surrounded by double quotes </p>
     * <p>If the RDFNode has a Resource value (URI) and toDescribeURIs is set
     * to true, the value of @getLabelForUri for the resource is returned,
     * surrounded by double quotes.</p>
     * Otherwise, the URI will be returned
     */
    private Pair<String, String> getStringForResult(RDFNode node, boolean getNodeLabel) {
        String result = "";
        Pair<String, String> res = new Pair<>("", "");
        boolean quote = false;

        if (node.isLiteral()) {
            Literal literal = node.asLiteral();
            Object literalValue = literal.getValue();

            if (literal.getDatatype() == null || literal.getDatatype().getJavaClass() == null) {
                result = EEASettings.parseForJson(
                        literal.getLexicalForm());
                quote = true;
            } else {
                Class<?> literalJavaClass = literal.getDatatype().getJavaClass();

                boolean BoolOrNumber = literalJavaClass.getName().equals(Boolean.class.getName())
                        || Number.class.isAssignableFrom(literalJavaClass);

                if (BoolOrNumber) {
                    result += literalValue;
                } else {
                    result = EEASettings.parseForJson(
                            literal.getLexicalForm());
                    quote = true;
                }
            }
            res = new Pair<>(result, literal.getLanguage());
        } else if (node.isResource()) {
            result = node.asResource().getURI();
            if (getNodeLabel) {
                //TODO: optimize
//                result = getLabelForUri(result);
            }
            if (Objects.isNull(result))
                result = node.asResource().toString();
            quote = true;
            res = new Pair<>(result, "");
        }
        //TODO: ?
        if (quote) {
            //result = "\"" + result + "\"";
        }
        return res;
    }

    /**
     * Returns the string value of the first of the properties in the
     * uriDescriptionList for the given resource (as an URI). In case the
     * resource does not have any of the properties mentioned, its URI is
     * returned. The value is obtained by querying the endpoint and the
     * endpoint is queried repeatedly until it gives a response (value or the
     * lack of it)
     * <p>
     * It is highly recommended that the list contains properties like labels
     * or titles, with test values.
     *
     * @param uri - the URI for which a label is required
     * @return a String value, either a label for the parameter or its value
     * if no label is obtained from the endpoint
     */
    private String getLabelForUri(String uri) {
        String result;

        if (harvester.getUriLabelCache().containsKey(uri)) {
            return harvester.getUriLabelCache().get(uri);
        }

        for (String prop : harvester.getUriDescriptionList()) {
            String innerQuery = "SELECT ?r WHERE {<" + uri + "> <" +
                    prop + "> ?r } LIMIT 1";

            try {
                Query query = QueryFactory.create(innerQuery);
                QueryExecution qExec = QueryExecutionFactory.sparqlService(
                        harvester.getRdfEndpoint(),
                        query);
                int retrycount = 0;
                while (retrycount++ < 5) {

                    //TODO : try finally?
                    try {
                        ResultSet results = qExec.execSelect();

                        if (results.hasNext()) {
                            QuerySolution sol = results.nextSolution();
                            result = EEASettings.parseForJson(
                                    sol.getLiteral("r").getLexicalForm());
                            if (!result.isEmpty()) {
                                harvester.putToUriLabelCache(uri, result);
                                return result;
                            }
                        } else {
                            break;
                        }
                    } catch (Exception e) {
                        logger.warn("Could not get label for uri {}. Retrying {}/5.",
                                uri, retrycount);
                    } finally {
                        qExec.close();
                    }
                }
            } catch (QueryParseException qpe) {
                logger.error("Exception for query {}. The label cannot be obtained",
                        innerQuery);
            }
        }
        return uri;
    }

}
