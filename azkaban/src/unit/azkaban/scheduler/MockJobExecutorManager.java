package azkaban.scheduler;

import java.util.ArrayList;

import org.joda.time.DateTime;

import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowExecutionHolder;
import azkaban.jobs.JobExecutorManager;

public class MockJobExecutorManager extends JobExecutorManager {
	
	private ArrayList<ExecutionRecord> executionList = new ArrayList<ExecutionRecord>();
	private RuntimeException throwException = null;
	
	public MockJobExecutorManager() 
	{
		super(null, null, null, null, null, 10);
	}
	
	public void setThrowException(RuntimeException throwException) {
		this.throwException = throwException;
	}
	
	@Override
    public void execute(String id, boolean ignoreDep) {
		DateTime time = new DateTime();
    	executionList.add(new ExecutionRecord(id, ignoreDep, time, throwException));
    	System.out.println("Running " + id + " at time " + time);
    	if (throwException != null) {
    		throw throwException;
    	}
    }

	@Override
    public void execute(ExecutableFlow flow) {
		System.out.println("Did not expect");
    }

	@Override
    public void execute(FlowExecutionHolder holder) {
		System.out.println("Did not expect");
    }
	
	public ArrayList<ExecutionRecord> getExecutionList() {
		return executionList;
	}
	
	public void clearExecutionList() {
		executionList.clear();
	}
	
    public class ExecutionRecord {
    	private final String id;
    	private final boolean ignoreDep;
    	private final DateTime startTime;
    	private final Exception throwException;
    	
    	public ExecutionRecord(String id, boolean ignoreDep, DateTime startTime) {
    		this(id, ignoreDep, startTime, null);
    	}
    	
    	public ExecutionRecord(String id, boolean ignoreDep, DateTime startTime, Exception throwException) {
    		this.id = id;
    		this.ignoreDep = ignoreDep;
    		this.startTime = startTime;
    		this.throwException = throwException;
    	}

		public String getId() {
			return id;
		}

		public boolean isIgnoreDep() {
			return ignoreDep;
		}

		public DateTime getStartTime() {
			return startTime;
		}

		public Exception getThrowException() {
			return throwException;
		}
    }
}