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

public class CommonProperties {
	private final String id;
	private Props prop;
	private long timestamp;
	
	public CommonProperties(String id, CommonProperties property) {
		this.id = id;
		this.prop = Props.clone(property.prop);
		this.timestamp = property.timestamp;
	}
	
	public CommonProperties(String id, Props prop) {
		this.id = id;
		this.prop = prop;
	}
	
	public String getId() {
		return id;
	}
	
	public Props getProps() {
		return prop;
	}

	public void setLastUpdateTime(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public long getLastUpdateTime() {
		return timestamp;
	}
	
	public boolean isHidden() {
		return prop.getBoolean("hidden", false);
	}
}
