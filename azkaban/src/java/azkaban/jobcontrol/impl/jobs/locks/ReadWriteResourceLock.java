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

import java.util.concurrent.locks.ReadWriteLock;

public class ReadWriteResourceLock extends AbstractJobLock
{
	public static final String WRITE = "write-lock";
	public static final String READ = "read-lock";
	private final ReadWriteLock _lock;
	private final Object _resource;
	private final boolean _write;
	
	public ReadWriteResourceLock( Object resource, ReadWriteLock lock, boolean write )
	{
		_lock = lock;
		_resource = resource;
		_write = write;
	}
	
	protected synchronized void lock() throws InterruptedException
	{
		if (_write)
		{
			_lock.writeLock().lock();
		}
		else
		{
			_lock.readLock().lock();
		}
	}
	
	protected synchronized void unlock()
	{
		if (_write) 
		{
			_lock.writeLock().unlock();
		}
		else 
		{
			_lock.readLock().unlock();
		}
		
	}
	
	public String toString() 
	{
		return "Resource Lock:" + _resource;
	}
}
