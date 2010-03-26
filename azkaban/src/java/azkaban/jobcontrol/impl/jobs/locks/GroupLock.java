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

import java.util.ArrayList;
import java.util.Collection;

/**
 * Groups a list of locks, attempts to reorder them, and then 
 * 
 * @author rpark
 *
 */
public class GroupLock extends AbstractJobLock 
{
	public final ArrayList< JobLock > _locks;
	
	public GroupLock(Collection<JobLock> list) 
	{
		_locks = new ArrayList<JobLock>(list);
	}
	
	protected synchronized void lock() throws InterruptedException
	{
		for (JobLock lock : _locks) 
		{
			lock.acquireLock();
		}
	}
	
	protected synchronized void unlock()
	{
		for (JobLock lock : _locks) 
		{
			lock.releaseLock();
		}
	}
	
	public String toString() 
	{
		String message = "";
		for(JobLock lock : _locks) 
		{
			message += lock + "\n";
		}
		
		return message;
	}
	
	public int numLocks() 
	{
		return _locks.size();
	}
}
