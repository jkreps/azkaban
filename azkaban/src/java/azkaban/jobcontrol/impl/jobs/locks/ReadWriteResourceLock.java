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
