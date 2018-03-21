package org.elasticsearch.app.support;

import com.google.common.collect.Maps;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.elasticsearch.app.EEASettings;
import org.elasticsearch.app.Harvester;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;

import java.util.*;

public class ESNormalizer {
    private final Harvester harvester;
    private Resource rs;
    private Set<Property> properties;
    private Model model;
    private boolean getPropLabel;

    private HashMap<String, Object> jsonMap = new HashMap<>();

    private boolean addUriForResource;
    private Map<String, Object> normalizeProp;
    private Map<String, Set<String>> whiteMap;
    private Map<String, Set<String>> blackMap;
    private Map<String, String> normalizeObj;
    private Boolean addLanguage;
    private String language;

    private final ESLogger logger = Loggers.getLogger(ESNormalizer.class);

    private Set<String> rdfLanguages = new HashSet<String>();
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

    public ESNormalizer(Resource rs, Set<Property> properties, Model model, boolean getPropLabel, Harvester harvester) {
        this.rs = rs;
        this.properties = properties;
        this.model = model;
        this.getPropLabel = getPropLabel;
        this.harvester = harvester;
    }

    public void process() {
        addUriForResource();

        for(Property prop: properties) {
            processProperty(prop);
        }
        addLanguage();

        normalizeMissing();
    }

    private void normalizeMissing() {
        for (Map.Entry<String, Object> it : normalizeMissing.entrySet()) {
            if (!jsonMap.containsKey(it.getKey())) {
                ArrayList<Object> res = new ArrayList<Object>();
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
                if(res.size() == 1)
                    jsonMap.put(it.getKey(), res.get(0));
                else
                    jsonMap.put(it.getKey(), res);
            }
        }
    }


    @SuppressWarnings("Duplicates")
    private void processProperty(Property prop) {
        NodeIterator niter = model.listObjectsOfProperty(rs,prop);
        String property = prop.toString();
        ArrayList<Object> results = new ArrayList<Object>();

        String currValue;
        //hasWorkflowState
        while (niter.hasNext()) {
            RDFNode node = niter.next();
            currValue = getStringForResult(node, getPropLabel);

            String shortValue = currValue;

            int currLen = currValue.length();
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
                if (!results.contains( normalizeObj.get(shortValue)  )){
                    results.add( normalizeObj.get(shortValue) );
                }
            } else {
                if (!results.contains(currValue)){
                    results.add(currValue);
                }
            }
        }

        // Do not index empty properties
        if (!results.isEmpty()) {

            if (normalizeProp.containsKey(property)) {
                Object norm_property = normalizeProp.get(property);
                if (norm_property instanceof String){
                    property = norm_property.toString();
                    if (jsonMap.containsKey(property)) {
                        Object temp = jsonMap.get(property);
                        if(temp instanceof List){
                            results.addAll((List) temp);
                            if( results.size() == 1)
                                jsonMap.put(property,results.get(0));
                            else
                                jsonMap.put(property,results );
                        } else {
                            jsonMap.put(property, results);
                        }
                    } else {
                        if(results.size() == 1)
                            jsonMap.put(property, results.get(0));
                        else
                            jsonMap.put(property, results);
                    }
                } else {
                    if (norm_property instanceof List<?>){

                        for (String norm_prop : ((List<String>) norm_property)) {

                            //TODO:
                            if (jsonMap.containsKey(norm_prop)) {
                                Object temp = jsonMap.get(norm_prop);
                                if(temp instanceof List){
                                    results.addAll((List) temp);
                                    if( results.size() == 1)
                                        jsonMap.put(norm_prop,results.get(0));
                                    else
                                        jsonMap.put(norm_prop,results );
                                } else {
                                    jsonMap.put(norm_prop, results);
                                }
                            } else {
                                if(results.size() == 1)
                                    jsonMap.put(norm_prop, results.get(0));
                                else
                                    jsonMap.put(norm_prop, results);
                            }
                        }

                    } else {
                        property = norm_property.toString();
                        if (jsonMap.containsKey(property)) {
                            Object temp = jsonMap.get(property);
                            //TODO:
                            if(temp instanceof List){
                                //((List) temp).addAll(results);
                                if(results.size() == 1)
                                    jsonMap.put(property, results.get(0));
                                else
                                    jsonMap.put(property, results.toArray());
                            } else {
                                if(results.size() == 1)
                                    jsonMap.put(property, results.get(0));
                                else
                                    jsonMap.put(property, results);

                            }
                            //jsonMap.get(property).addAll(results);
                        } else {
                            if(results.size() == 1)
                                jsonMap.put(property, results.get(0));
                            else
                                jsonMap.put(property, results);

                        }
                        logger.error("Normalizer error:" , norm_property);
                    }
                }
            } else {
                jsonMap.put(property, results);
            }
        }

    }

    private void addUriForResource() {
        ArrayList<Object> results = new ArrayList<Object>();
        if(addUriForResource) {
            results.add( rs.toString() );
            if(results.size() == 1)
                jsonMap.put("about", results.get(0));
            else
                jsonMap.put("about", results);

        }
    }

    private void addLanguage() {
        if(addLanguage) {
            HashSet<Property> allProperties = new HashSet<Property>();

            StmtIterator it = model.listStatements();
            while(it.hasNext()) {
                Statement st = it.nextStatement();
                Property prop = st.getPredicate();

                allProperties.add(prop);
            }

            for(Property prop: allProperties) {
                String property = prop.toString();
                NodeIterator niter = model.listObjectsOfProperty(rs,prop);
                String lang;

                while (niter.hasNext()) {
                    RDFNode node = niter.next();
                    if (addLanguage) {
                        if (node.isLiteral()) {
                            lang = node.asLiteral().getLanguage();
                            if (!lang.isEmpty()) {
                                rdfLanguages.add( lang );
                            }
                        }
                    }
                }
            }

            if(rdfLanguages.isEmpty() && !language.isEmpty())
                rdfLanguages.add(language);
            if(!rdfLanguages.isEmpty()){
                if(rdfLanguages.size() == 1 ){
                    Iterator iter = rdfLanguages.iterator();
                    Object first = iter.next();
                    jsonMap.put("language", first);
                } else {
                    jsonMap.put("language", rdfLanguages);
                }
            }


        }
    }

    public HashMap<String,Object> getJsonMap() {
        return jsonMap;
    }

    /**
     * Builds a String result for Elastic Search from an RDFNode
     * @param node An RDFNode representing the value of a property for a given
     * resource
     * @return If the RDFNode has a Literal value, among Boolean, Byte, Double,
     * Float, Integer Long, Short, this value is returned, converted to String
     * <p>If the RDFNode has a String Literal value, this value will be
     * returned, surrounded by double quotes </p>
     * <p>If the RDFNode has a Resource value (URI) and toDescribeURIs is set
     * to true, the value of @getLabelForUri for the resource is returned,
     * surrounded by double quotes.</p>
     * Otherwise, the URI will be returned
     */
    private String getStringForResult(RDFNode node, boolean getNodeLabel) {
        String result = "";
        boolean quote = false;

        if(node.isLiteral()) {
            Object literalValue = node.asLiteral().getValue();
            try {
                Class<?> literalJavaClass = node.asLiteral()
                        .getDatatype()
                        .getJavaClass();

                if (literalJavaClass.equals(Boolean.class)
                        || Number.class.isAssignableFrom(literalJavaClass)) {

                    result += literalValue;
                }	else {
                    result =	EEASettings.parseForJson(
                            node.asLiteral().getLexicalForm());
                    quote = true;
                }
            } catch (java.lang.NullPointerException npe) {
                result = EEASettings.parseForJson(
                        node.asLiteral().getLexicalForm());
                quote = true;
            }

        } else if(node.isResource()) {
            result = node.asResource().getURI();
            if(getNodeLabel) {
                result = getLabelForUri(result);
            }
            quote = true;
        }
        //TODO: ?
        if(quote) {
            //result = "\"" + result + "\"";
        }
        return result;
    }

    /**
     * Returns the string value of the first of the properties in the
     * uriDescriptionList for the given resource (as an URI). In case the
     * resource does not have any of the properties mentioned, its URI is
     * returned. The value is obtained by querying the endpoint and the
     * endpoint is queried repeatedly until it gives a response (value or the
     * lack of it)
     *
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

        for(String prop:harvester.getUriDescriptionList()) {
            String innerQuery = "SELECT ?r WHERE {<" + uri + "> <" +
                    prop + "> ?r } LIMIT 1";

            try {
                Query query = QueryFactory.create(innerQuery);
                QueryExecution qExec = QueryExecutionFactory.sparqlService(
                        harvester.getRdfEndpoint(),
                        query);
                boolean keepTrying = true;
                while(keepTrying) {
                    keepTrying = false;

                    //TODO : try finally?
                    try {
                        ResultSet results = qExec.execSelect();

                        if(results.hasNext()) {
                            QuerySolution sol = results.nextSolution();
                            result = EEASettings.parseForJson(
                                    sol.getLiteral("r").getLexicalForm());
                            if(!result.isEmpty()) {
                                harvester.putToUriLabelCache(uri, result);
                                return result;
                            }
                        }
                    } catch(Exception e) {
                        keepTrying = true;
                        //TODO:LOG - DONE
                        logger.warn("Could not get label for uri {}. Retrying.",
                                uri);
                    } finally { qExec.close();}
                }
            } catch (QueryParseException qpe) {
                //TODO:LOG - DONE
                logger.error("Exception for query {}. The label cannot be obtained",
                        innerQuery);
            }
        }
        return uri;
    }

}
