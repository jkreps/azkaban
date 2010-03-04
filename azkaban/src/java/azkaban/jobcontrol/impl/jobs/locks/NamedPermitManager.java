package azkaban.jobcontrol.impl.jobs.locks;

import java.util.Hashtable;
import java.util.concurrent.Semaphore;

public class NamedPermitManager {
	private final Hashtable<String, Permit> _namedPermitManager;
	
	public NamedPermitManager() {
		_namedPermitManager = new Hashtable<String, Permit>();
	}
	
	public void createNamedPermit( String name, int totalPermits ) {
		_namedPermitManager.put(name, new Permit(totalPermits));
	}
	
	public PermitLock getNamedPermit( String name, int numPermits ) {
		Permit permit = _namedPermitManager.get(name);
		if ( permit == null ) {
			return null;
		}
		
		return new PermitLock( name, permit._semaphore, numPermits, permit._totalPermits );
	}
	
	private class Permit {
		private final Semaphore _semaphore;
		private final int _totalPermits;
		
		private Permit( int totalPermits ) {
			_totalPermits = totalPermits;
			_semaphore = new Semaphore( totalPermits, true );
		}
	}
	
}
