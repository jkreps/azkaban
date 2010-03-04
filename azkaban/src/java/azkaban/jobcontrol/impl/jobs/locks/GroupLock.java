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
