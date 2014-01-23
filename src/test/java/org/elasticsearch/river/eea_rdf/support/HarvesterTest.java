package org.elasticsearch.river.eea_rdf.support;

import junit.framework.TestCase;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.elasticsearch.river.eea_rdf.support.Harvester;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;


public class HarvesterTest extends TestCase {

	@Test
	public void testMapToString() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException, ClassNotFoundException, NoSuchMethodException, SecurityException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("mapToString", Map.class);
		method.setAccessible(true);
		Harvester harvester = mock(Harvester.class);
		
		Map<String, ArrayList<String>> testMap = new HashMap<String, ArrayList<String>>();
		//No values
		ArrayList<String> values = new ArrayList<String>();
		testMap.put("no_val_prop", values);
		String result = "{\"no_val_prop\" : []}\n";
		assertEquals(result, method.invoke(harvester, testMap));
		//One value
		testMap.clear();
		values.clear();
		values.add("onevalue");
		testMap.put("one_val_prop", values);
		result = "{\"one_val_prop\" : onevalue}\n";
		assertEquals(result, method.invoke(harvester, testMap));
		//More Values
		testMap.clear();
		values.clear();
		values.add("first");
		values.add("second");
		values.add("third");
		testMap.put("more_vals_prop", values);
		result = "{\"more_vals_prop\" : [first, second, third]}\n";
		assertEquals(result, method.invoke(harvester, testMap));

	}
	
	@Test
	public void testgetLabelForUri() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		method.setAccessible(true);
		
		Field uDL = test.getClass().getDeclaredField("uriDescriptionList");
		uDL.setAccessible(true);
		Field endpt = test.getClass().getDeclaredField("rdfEndpoint");
		endpt.setAccessible(true);
		
		List<String> uriDescriptionList = new ArrayList<String>();
		uriDescriptionList.add("http://purl.org/dc/terms/title");
		uriDescriptionList.add("http://www.w3.org/2000/01/rdf-schema#label");
		
		Harvester harvester = mock(Harvester.class);
		uDL.set(harvester, uriDescriptionList);
		endpt.set(harvester, "http://semantic.eea.europa.eu/sparql");
		
		assertEquals("Webpage", 
				method.invoke(harvester, "http://purl.org/ontology/bibo/Webpage"));
		
	}
	
	@Test
	public void testgetStringForResultforBoolean() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);

		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(true);
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Boolean.class;
			}
		});

		assertEquals("true", 
				method.invoke(harvester, literal));
		
	}
	
	@Test
	public void testgetStringForResultforByte() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		
		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(5);
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Byte.class;
			}
		});

		assertEquals("5", 
				method.invoke(harvester, literal));
		
	}
	
	@Test
	public void testgetStringForResultforDouble() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		
		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(new Double(3.141592653589793238));
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Double.class;
			}
		});

		assertEquals("3.141592653589793", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforFloat() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		
		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(new Float(3.141592653589793238));
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Float.class;
			}
		});
		assertEquals("3.1415927", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforInteger() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		
		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(32767);
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Integer.class;
			}
		});
		assertEquals("32767", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforLong() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		
		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(50000);
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Long.class;
			}
		});
		assertEquals("50000", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforShort() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);

		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getValue()).thenReturn(32767);
		
		RDFDatatype type = mock(RDFDatatype.class);
		when(litValue.getDatatype()).thenReturn(type);
		
		when(type.getJavaClass()).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock iom) throws Throwable {
				return Short.class;
			}
		});
		assertEquals("32767", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforFreeText() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		Method othermethod = test.getClass().getDeclaredMethod("getLabelForUri", String.class);
		othermethod.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);

		//Test for Literal node
		RDFNode literal = mock(RDFNode.class);
		when(literal.isLiteral()).thenReturn(true);
		
		Literal litValue = mock(Literal.class);
		when(literal.asLiteral()).thenReturn(litValue);
		when(litValue.getLexicalForm()).thenReturn("\"This is a \nfree te\rxt");
		
		assertEquals("\"'This is a  free te xt\"", 
				method.invoke(harvester, literal));
		
	}

	@Test
	public void testgetStringForResultforResource() throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
		Object test = Class.forName("org.elasticsearch.river.eea_rdf.support.Harvester").newInstance();
		Method method = test.getClass().getDeclaredMethod("getStringForResult", RDFNode.class);
		method.setAccessible(true);
		
		Field tDU = test.getClass().getDeclaredField("toDescribeURIs");
		tDU.setAccessible(true);
		Field uDL = test.getClass().getDeclaredField("uriDescriptionList");
		uDL.setAccessible(true);
		Field endpt = test.getClass().getDeclaredField("rdfEndpoint");
		endpt.setAccessible(true);
				
		Harvester harvester = mock(Harvester.class);
		tDU.set(harvester, true);
		List<String> uriDescriptionList = new ArrayList<String>();
		uriDescriptionList.add("http://www.w3.org/2000/01/rdf-schema#label");
		uDL.set(harvester, uriDescriptionList);
		endpt.set(harvester, "http://semantic.eea.europa.eu/sparql");
		
		//Test for Resource node
		RDFNode resource = mock(RDFNode.class);
		when(resource.isLiteral()).thenReturn(false);
		when(resource.isResource()).thenReturn(true);
		
		Resource uriValue = mock(Resource.class);
		when(resource.asResource()).thenReturn(uriValue);
		when(uriValue.getURI()).thenReturn("http://purl.org/ontology/bibo/Webpage");
		
		assertEquals("\"Webpage\"", 
				method.invoke(harvester, resource));
		
		tDU.set(harvester, false);
		assertEquals("\"http://purl.org/ontology/bibo/Webpage\"", 
				method.invoke(harvester, resource));
		
	}
}
