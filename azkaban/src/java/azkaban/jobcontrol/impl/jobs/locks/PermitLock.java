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
