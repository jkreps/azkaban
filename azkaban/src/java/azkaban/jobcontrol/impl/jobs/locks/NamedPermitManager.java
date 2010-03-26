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

package azkaban.jobcontrol.impl.jobs.locks;

import java.util.Hashtable;
import java.util.concurrent.Semaphore;

public class NamedPermitManager {
	private final Hashtable<String, Permit> _namedPermitManager;
	
	public NamedPermitManager() {
		_namedPermitManager = new Hashtable<String, Permit>();
	}
	
	public void createNamedPermit( String name, int totalPermits ) {
		_namedPermitManager.put(name, new Permit(totalPermits));
	}
	
	public PermitLock getNamedPermit( String name, int numPermits ) {
		Permit permit = _namedPermitManager.get(name);
		if ( permit == null ) {
			return null;
		}
		
		return new PermitLock( name, permit._semaphore, numPermits, permit._totalPermits );
	}
	
	private class Permit {
		private final Semaphore _semaphore;
		private final int _totalPermits;
		
		private Permit( int totalPermits ) {
			_totalPermits = totalPermits;
			_semaphore = new Semaphore( totalPermits, true );
		}
	}
	
}
