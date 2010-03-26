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

import java.util.concurrent.TimeUnit;

public abstract class AbstractJobLock implements JobLock {
	private Status _status = Status.UNUSED;
	private long _lockAcquireStartTime = -1;
	private long _lockAcquireEndTime = -1;
	private long _lockReleasedTime = -1;
	
	public synchronized void acquireLock() throws InterruptedException
	{
		if (_status == Status.UNUSED) 
		{
			_status = Status.ACQUIRING_LOCK;
			_lockAcquireStartTime = System.currentTimeMillis();
			lock();
			_lockAcquireEndTime = System.currentTimeMillis();
			_status = Status.ACQUIRED_LOCK;
		}
	}
	
	public synchronized void releaseLock() 
	{
		if (_status == Status.ACQUIRED_LOCK)
		{
			unlock();
			_lockReleasedTime = System.currentTimeMillis();
			_status = Status.RELEASED_LOCK;
		}
	}
	
	public long getLockAcquireTime(TimeUnit unit)
	{
		return convertTime(unit, getTimeDiff(_lockAcquireStartTime, _lockAcquireEndTime));
	}
	
	public long getLockHeldTime(TimeUnit unit)
	{
		return convertTime(unit, getTimeDiff(_lockAcquireEndTime, _lockReleasedTime));
	}
	
	public long getTotalLockTime(TimeUnit unit)
	{
		return convertTime(unit, getTimeDiff(_lockAcquireStartTime, _lockReleasedTime));
	}
	
	public Status getStatus()
	{
		return _status;
	}
	
	protected abstract void lock() throws InterruptedException;
	
	protected abstract void unlock();
	
	private long convertTime(TimeUnit unit, long time) 
	{
		if ( time == -1l ) 
		{
			return -1l;
		}
		
		if ( unit == null ) {
			unit = TimeUnit.MILLISECONDS;
		}
		
		return unit.convert(time, TimeUnit.MILLISECONDS);
	}
	
	private long getTimeDiff(long start, long end) 
	{
		if ( start == -1) 
		{
			return -1;
		}
		else if ( end == -1 ) 
		{
			return System.currentTimeMillis() - start;
		}
		
		return end - start;
	}
}
