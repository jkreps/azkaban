/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.util.json;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * There are many implementations of JSON writers/parsers. This is to wrap them so we
 * aren't so sticky on any given implementation. Also wraps all the exception handling.
 * This isolates this dependency in case we want to change it in the future.
 * 
 * The current implementation uses org.json since it's simple and writes nice looking json.
 * 
 * @author Richard Park
 */
public class JSONUtils {
	
	/**
	 * The constructor.
	 */
	public JSONUtils() {
	}
	
	/**
	 * Takes a reader to stream the JSON string. The reader is not wrapped in a BufferReader
	 * so it is up to the user to employ such optimizations if so desired.
	 * 
	 * The results will be Maps, Lists and other Java mapping of Json types (i.e. String, Number, Boolean).
	 * 
	 * @param reader
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> fromJSONStream(Reader reader) throws Exception {
		JSONObject jsonObj = new JSONObject(new JSONTokener(reader));
		Map<String, Object> results = createObject(jsonObj);
		
		return results;
	}
	
	/**
	 * Converts a json string to Objects.
	 * 
	 * The results will be Maps, Lists and other Java mapping of Json types (i.e. String, Number, Boolean).
	 * 
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> fromJSONString(String str) throws Exception {
		JSONObject jsonObj = new JSONObject(str);
		Map<String, Object> results = createObject(jsonObj);
		return results;
	}

	/**
	 * Recurses through the json object to create a Map/List/Object equivalent.
	 * 
	 * @param obj
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> createObject(JSONObject obj) {
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
		
		Iterator<String> iterator = obj.keys();
		while(iterator.hasNext()) {
			String key = iterator.next();
			Object value = null;
			try {
				value = obj.get(key);
			} catch (JSONException e) {
				// Since we get the value from the key given by the JSONObject, 
				// this exception shouldn't be thrown.
			}
			
			if (value instanceof JSONArray) {
				value = createArray((JSONArray)value);
			}
			else if (value instanceof JSONObject) {
				value = createObject((JSONObject)value);
			}
			
			map.put(key, value);
		}

		return map;
	}
	
	/**
	 * Recurses through the json object to create a Map/List/Object equivalent.
	 * 
	 * @param obj
	 * @return
	 */
	private List<Object> createArray(JSONArray array) {
		ArrayList<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); ++i) {
			Object value = null;
			try {
				value = array.get(i);
			} catch (JSONException e) {
				// Ugh... JSON's over done exception throwing.
			}
			
			if (value instanceof JSONArray) {
				value = createArray((JSONArray)value);
			}
			else if (value instanceof JSONObject) {
				value = createObject((JSONObject)value);
			}
			
			list.add(value);
		}
		
		return list;
	}
	
	/**
	 * Creates a json string from Map/List/Primitive object.
	 * 
	 * @param obj
	 * @return
	 */
	public String toJSONString(Map<String, Object> obj) {
		JSONObject jsonObj = new JSONObject(obj);
		try {
			return jsonObj.toString();
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * Creates a json pretty string from Map/List/Primitive object
	 * 
	 * @param obj
	 * @param indent
	 * @return
	 */
	public String toJSONString(Map<String, Object> obj, int indent) {
		JSONObject jsonObj = new JSONObject(obj);
		try {
			return jsonObj.toString(4);
		} catch (Exception e) {
			return "";
		}
	}
}