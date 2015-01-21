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

package azkaban.workflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import azkaban.common.utils.Props;

/**
 * Basic unit of work.
 * 
 * @author Richard B Park
 */
public class JobUnit {
	public static final String DEPENDENCY_KEY = "dependencies";
	public static final String TYPE_KEY = "type";
	
	private final String id;
	private final Props prop;
	private final HashSet<String> dependencies;
	private final HashSet<String> dependents;
	private final String type;
	
	public JobUnit(String id, JobUnit toClone) {
		this.id = id;
		this.prop = Props.clone(toClone.getProps());
		this.type = toClone.getType();
		this.dependencies = new HashSet<String>(toClone.getDependencies());
		this.dependents = new HashSet<String>(toClone.getDependencies());
	}
	
	public JobUnit(String id, Props prop) {
		this.id = id;
		this.prop = prop;
		this.type = prop.getString(TYPE_KEY);
		this.dependencies = prop.containsKey(DEPENDENCY_KEY) ? 
				new HashSet<String>(prop.getStringList(DEPENDENCY_KEY)) : 
				new HashSet<String>(); 
		
		this.dependents = new HashSet<String>();
	}
	
	public String getId() {
		return id;
	}
	
	public Props getProps() {
		return prop;
	}
	
	public String getType() {
		return type;
	}
	
	public void addDependent(String dependent) {
		dependents.add(dependent);
	}
	
	public List<String> getDependencies() {
		return new ArrayList<String>(dependencies);
	}

	public List<String> getDependents() {
		return new ArrayList<String>(dependents);
	}
	
}
