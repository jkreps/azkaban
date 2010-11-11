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

import azkaban.common.utils.Props;

/**
 * A discrete job to be run.
 *  
 * @author Richard B Park
 */
public class Job extends WorkUnit {
	public static final String REFERENCE_KEY = "reference";
	public static final String TYPE_KEY = "type";
	public static final String DEPENDENCY_KEY = "dependencies";
	private long timestamp;
	
	public Job(String id, Job toClone) {
		super(id, toClone);
		this.timestamp = toClone.timestamp;
	}
	
	public Job(String id, Props prop) {
		super(id, prop);
	}

	public String getType() {
		return getProps().get(TYPE_KEY);
	}
	
	public String getReference() {
		return getProps().get(REFERENCE_KEY);
	}
	
	public void setLastUpdateTime(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public long getLastUpdateTime() {
		return timestamp;
	}
}
