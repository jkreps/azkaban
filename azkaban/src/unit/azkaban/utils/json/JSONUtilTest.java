package azkaban.utils.json;

import java.util.List;
import java.util.Map;
import java.io.StringReader;

import org.junit.Test;
import static org.junit.Assert.*;
import azkaban.util.json.JSONUtils;

/**
 * Tests the serialization and deserialization of JSON with the base of Map.
 * 
 * @author rpark
 *
 */
public class JSONUtilTest {
    @SuppressWarnings("unchecked")
	@Test
    public void objectFromString() throws Exception {
    	JSONUtils utils = new JSONUtils();
    	
    	String jsonTest = "{\"test\":[1,2,\"tree\"], \"test2\":{\"a\":\"b\"}, \"test4\":\"bye\"}";
    	Object obj = null;
    	try {
    		obj = utils.fromJSONString(jsonTest);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
      	
    	assertTrue("Testing for instance Map", obj instanceof Map);
    	if (obj instanceof Map) {
    		Map mobj = (Map)obj;
    		assertEquals("Main map size should be 3", mobj.size(), 3);
    		assertTrue("Testing for key test", mobj.containsKey("test"));
    		assertTrue("Testing for key test2", mobj.containsKey("test2"));
    		assertTrue("Testing for key test4", mobj.containsKey("test4"));

    		Object testObj = mobj.get("test");
    		assertTrue(testObj instanceof List);
    		List ltestObj = (List)testObj;
    		assertEquals("List size should be 3", ltestObj.size(), 3);
    		assertEquals("First list item should be 1", ltestObj.get(0), 1);
    		assertEquals("First list item should be 2", ltestObj.get(1), 2);
    		assertEquals("First list item should be tree", ltestObj.get(2), "tree");

    		Object test2Obj = mobj.get("test2");
    		assertTrue("test2 should be a map", test2Obj instanceof Map);
    		Map mtest2 = (Map)test2Obj;
    		assertEquals("test2 size should be 1", mtest2.size(), 1);
    		assertEquals("test2 a = b", mtest2.get("a"), "b");

    		Object test4Obj = mobj.get("test4");
    		assertTrue("test4 is a string", test4Obj instanceof String);
    		assertEquals("test4 content is bye", test4Obj, "bye");
    	}
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void objectFromStream() throws Exception {
    	JSONUtils utils = new JSONUtils();
    	
    	String jsonTest = "{\"test\":[1,2,\"tree\"], \"test2\":{\"a\":\"b\"}, \"test4\":\"bye\"}";
    	StringReader reader = new StringReader(jsonTest);
    	Object obj = null;
    	try {
    		obj = utils.fromJSONStream(reader);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
      	
    	assertTrue("Testing for instance Map", obj instanceof Map);
    	if (obj instanceof Map) {
    		Map mobj = (Map)obj;
    		assertEquals("Main map size should be 3", mobj.size(), 3);
    		assertTrue("Testing for key test", mobj.containsKey("test"));
    		assertTrue("Testing for key test2", mobj.containsKey("test2"));
    		assertTrue("Testing for key test4", mobj.containsKey("test4"));

    		Object testObj = mobj.get("test");
    		assertTrue(testObj instanceof List);
    		List ltestObj = (List)testObj;
    		assertEquals("List size should be 3", ltestObj.size(), 3);
    		assertEquals("First list item should be 1", ltestObj.get(0), 1);
    		assertEquals("First list item should be 2", ltestObj.get(1), 2);
    		assertEquals("First list item should be tree", ltestObj.get(2), "tree");

    		Object test2Obj = mobj.get("test2");
    		assertTrue("test2 should be a map", test2Obj instanceof Map);
    		Map mtest2 = (Map)test2Obj;
    		assertEquals("test2 size should be 1", mtest2.size(), 1);
    		assertEquals("test2 a = b", mtest2.get("a"), "b");

    		Object test4Obj = mobj.get("test4");
    		assertTrue("test4 is a string", test4Obj instanceof String);
    		assertEquals("test4 content is bye", test4Obj, "bye");
    	}
    }
    
    @SuppressWarnings("unchecked")
	@Test
    public void backAndForth() throws Exception {
    	JSONUtils utils = new JSONUtils();
    	
    	String jsonTest = "{\"test\":[1,2,\"tree\"], \"test2\":{\"a\":\"b\"}, \"test4\":\"bye\"}";
    	Map oldObj = null;
    	try {
    		oldObj = utils.fromJSONString(jsonTest);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    	System.out.println(oldObj);
    	String returnString = utils.toJSONString(oldObj);
    	System.out.println(returnString);
    	Map obj = null;
    	try {
    		obj = utils.fromJSONString(returnString);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    	assertTrue("Testing for instance Map", obj instanceof Map);
    	if (obj instanceof Map) {
    		Map mobj = (Map)obj;
    		assertEquals("Main map size should be 3", mobj.size(), 3);
    		assertTrue("Testing for key test", mobj.containsKey("test"));
    		assertTrue("Testing for key test2", mobj.containsKey("test2"));
    		assertTrue("Testing for key test4", mobj.containsKey("test4"));

    		Object testObj = mobj.get("test");
    		assertTrue(testObj instanceof List);
    		List ltestObj = (List)testObj;
    		assertEquals("List size should be 3", ltestObj.size(), 3);
    		assertEquals("First list item should be 1", ltestObj.get(0), 1);
    		assertEquals("First list item should be 2", ltestObj.get(1), 2);
    		assertEquals("First list item should be tree", ltestObj.get(2), "tree");

    		Object test2Obj = mobj.get("test2");
    		assertTrue("test2 should be a map", test2Obj instanceof Map);
    		Map mtest2 = (Map)test2Obj;
    		assertEquals("test2 size should be 1", mtest2.size(), 1);
    		assertEquals("test2 a = b", mtest2.get("a"), "b");

    		Object test4Obj = mobj.get("test4");
    		assertTrue("test4 is a string", test4Obj instanceof String);
    		assertEquals("test4 content is bye", test4Obj, "bye");
    	}
    }
}