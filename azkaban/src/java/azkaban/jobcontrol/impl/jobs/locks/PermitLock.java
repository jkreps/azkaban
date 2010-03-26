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

import java.util.concurrent.Semaphore;

public class PermitLock extends AbstractJobLock 
{
	private final Semaphore _permitPool;
	private final String _permitName;
	private final int _totalPermits;
	private final int _numPermits;
	
	public PermitLock(String permitName, Semaphore permitPool, int numPermits, int totalPermits) 
	{
		_permitPool = permitPool;
		_totalPermits = totalPermits;
		_numPermits = numPermits;
		_permitName = permitName;
	}
	
	public int getDesiredNumPermits() 
	{
		return _numPermits;
	}
	
	protected synchronized void lock() throws InterruptedException
	{
        _permitPool.acquire(_numPermits);
    }
	
	protected synchronized void unlock()
	{
		_permitPool.release(_numPermits);
	}

	public String toString()
	{
		return "Permit:" + _permitName + " NumPermits:" + _numPermits;
	}
	
	public int getTotalNumberOfPermits()
	{
		return _totalPermits;
	}
}
