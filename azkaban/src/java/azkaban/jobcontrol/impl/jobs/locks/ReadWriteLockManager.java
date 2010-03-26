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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockManager {
	private final Hashtable<Object, ReadWriteLock > _readWriteLockMap;
	private boolean _isFair = true;
	
	public ReadWriteLockManager() 
	{
		_readWriteLockMap = new Hashtable<Object, ReadWriteLock >();
	}
	
	public ReadWriteResourceLock getReadLock(Object resource) 
	{
		ReadWriteLock rwl = _readWriteLockMap.get(resource);
		if(rwl == null) 
		{
			rwl = new ReentrantReadWriteLock(_isFair);
			_readWriteLockMap.put(resource, rwl);
		}
		
		return new ReadWriteResourceLock( resource, rwl, false );
	}
	
	public ReadWriteResourceLock getWriteLock(Object resource) 
	{
		ReadWriteLock rwl = _readWriteLockMap.get(resource);
		if (rwl == null) 
		{
			rwl = new ReentrantReadWriteLock(_isFair);
			_readWriteLockMap.put(resource, rwl);
		}
		
		return new ReadWriteResourceLock(resource, rwl, true);
	}
}
