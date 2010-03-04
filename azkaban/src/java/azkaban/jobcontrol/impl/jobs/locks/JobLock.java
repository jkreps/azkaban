package azkaban.jobcontrol.impl.jobs.locks;

import java.util.concurrent.TimeUnit;

public interface JobLock {
	public static enum Status 
	{
		UNUSED,
		ACQUIRING_LOCK,
		ACQUIRED_LOCK,
		RELEASED_LOCK
	}
	
	public void acquireLock() throws InterruptedException;
	public void releaseLock();
	
	public long getLockAcquireTime(TimeUnit unit);
	
	public long getLockHeldTime(TimeUnit unit);
	
	public long getTotalLockTime(TimeUnit unit);
	
	public Status getStatus();
}
