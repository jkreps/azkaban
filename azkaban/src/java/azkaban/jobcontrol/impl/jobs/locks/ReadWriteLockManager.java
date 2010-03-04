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
