package azkaban.scheduler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormat;

import azkaban.jobs.JobExecutionException;
import azkaban.jobs.JobExecutorManager;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;


/**
 * The ScheduleManager stores and executes the schedule. It uses a single thread instead
 * and waits until correct loading time for the job. It will not remove the job from the
 * schedule when it is run, which can potentially allow the job to and overlap each other.
 * 
 * @author Richard
 */
public class ScheduleManager {
	private static Logger logger = Logger.getLogger(ScheduleManager.class);

    private final DateTimeFormatter _dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy HH:mm:ss:SSS");
	private ScheduleLoader loader;
    private Map<String, ScheduledJob> scheduleIDMap = new LinkedHashMap<String, ScheduledJob>(); 
    private final ScheduleRunner runner;
    private final JobExecutorManager executionManager;
    
    /**
     * Give the schedule manager a loader class that will properly load the schedule.
     * 
     * @param loader
     */
    public ScheduleManager(
    		JobExecutorManager executionManager,
    		ScheduleLoader loader) 
    {
    	this.executionManager = executionManager;
    	this.loader = loader;
    	this.runner = new ScheduleRunner();
    	
    	List<ScheduledJob> scheduleList = loader.loadSchedule();
    	for (ScheduledJob job: scheduleList) {
    		internalSchedule(job);
    	}
    	
    	this.runner.start();
    }
    
    /**
     * Shutdowns the scheduler thread. After shutdown, it may not be safe to use it again.
     */
    public void shutdown() {
    	this.runner.shutdown();
    }
    
    /**
     * Retrieves a copy of the list of schedules.
     * 
     * @return
     */
    public synchronized List<ScheduledJob> getSchedule() {
    	return runner.getSchedule();
    }

    /**
     * Returns the scheduled job for the job name
     * 
     * @param id
     * @return
     */
    public ScheduledJob getSchedule(String id) {
    	return scheduleIDMap.get(id);
    }
    
    /**
     * Removes the job from the schedule if it exists.
     * 
     * @param id
     */
    public synchronized void removeScheduledJob(String id) {
    	ScheduledJob job = scheduleIDMap.get(id);
    	scheduleIDMap.remove(id);
    	runner.removeScheduledJob(job);
    	
    	loader.saveSchedule(getSchedule());
    }
    
    public void schedule(String jobId,
            DateTime dateTime,
            ReadablePeriod period,
            boolean ignoreDep) {
		logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(dateTime)
		+ " with a period of " + PeriodFormat.getDefault().print(period));
		schedule(new ScheduledJob(jobId, dateTime, period, ignoreDep));
	}
    
    /**
     * Schedule the job
     * @param jobId
     * @param date
     * @param ignoreDep
     */
    public void schedule(String jobId, DateTime date, boolean ignoreDep) {
        logger.info("Scheduling job '" + jobId + "' for " + _dateFormat.print(date));
        schedule(new ScheduledJob(jobId, date, ignoreDep));
    }
    
    /**
     * Schedules the job, but doesn't save the schedule afterwards.
     * @param job
     */
    private synchronized void internalSchedule(ScheduledJob job) {
    	ScheduledJob existing = scheduleIDMap.get(job.getId());
    	job.updateTime();
    	if (existing != null) {
    		this.runner.removeScheduledJob(existing);
    	}
    	
		this.runner.addScheduledJob(job);
    	scheduleIDMap.put(job.getId(), job);
    }
    
    /**
     * Adds a job to the schedule.
     * 
     * @param job
     */
    public synchronized void schedule(ScheduledJob job) {
    	internalSchedule(job);
    	saveSchedule();
    }
    
    /**
     * Save the schedule
     */
    private void saveSchedule() {
    	loader.saveSchedule(getSchedule());
    }
    
    /**
     * Thread that simply invokes the running of jobs when the schedule is
     * ready.
     * 
     * @author Richard Park
     *
     */
    public class ScheduleRunner extends Thread {
    	private final PriorityBlockingQueue<ScheduledJob> schedule;
    	private AtomicBoolean stillAlive = new AtomicBoolean(true);

        	// Five minute minimum intervals
    	private static final int TIMEOUT_MS = 300000;
    	
    	public ScheduleRunner() {
    		schedule = new PriorityBlockingQueue<ScheduledJob>(1, new ScheduleComparator());
    	}
    	
    	public void shutdown() {
    		logger.error("Shutting down scheduler thread");
    		stillAlive.set(false);
    		this.interrupt();
    	}
    	
    	/**
    	 * Return a list of scheduled job
    	 * @return
    	 */
    	public synchronized List<ScheduledJob> getSchedule() {
    		return new ArrayList<ScheduledJob>(schedule);
    	}
    	
    	/**
    	 * Adds the job to the schedule and then interrupts so it will update its wait time.
    	 * @param job
    	 */
        public synchronized void addScheduledJob(ScheduledJob job) {
        	logger.info("Adding " + job + " to schedule.");
        	schedule.add(job);
            MonitorImpl.getInternalMonitorInterface().workflowEvent(null, 
                    System.currentTimeMillis(),
                    WorkflowAction.SCHEDULE_WORKFLOW, 
                    WorkflowState.NOP,
                    job.getId());

        	this.interrupt();
        }

        /**
         * Remove scheduled jobs. Does not interrupt.
         * 
         * @param job
         */
        public synchronized void removeScheduledJob(ScheduledJob job) {
        	logger.info("Removing " + job + " from the schedule.");
        	schedule.remove(job);
            MonitorImpl.getInternalMonitorInterface().workflowEvent(null, 
                    System.currentTimeMillis(),
                    WorkflowAction.UNSCHEDULE_WORKFLOW,
                    WorkflowState.NOP,
                    job.getId());
        	// Don't need to interrupt, because if this is originally on the top of the queue,
        	// it'll just skip it.
        }
        
        public void run() {
        	while(stillAlive.get()) {
        		synchronized (this) {
        			try {
        				ScheduledJob job = schedule.peek();
	    	    		
	    	    		if (job == null) {
	    	    			// If null, wake up every minute or so to see if there's something to do.
	    	    			// Most likely there will not be.
	    	    			try {
	    	    				this.wait(TIMEOUT_MS);
	    	    			} catch (InterruptedException e) {
	    	    				// interruption should occur when items are added or removed from the queue.
	    	    			}
	    	    		}
	    	    		else {
	    	    			// We've passed the job execution time, so we will run.
	    	    			if (!job.getScheduledExecution().isAfterNow()) {
	    	    				// Run job. The invocation of jobs should be quick.
	    	    				ScheduledJob runningJob = schedule.poll();
	    	    				logger.info("Scheduler attempting to run " + runningJob.getId());
	
	    	    				// Execute the job here
	    	    				try {
	    	    					executionManager.execute(runningJob.getId(), runningJob.isDependencyIgnored());
	    	    				} catch (JobExecutionException e) {
	    	    					logger.info("Could not run job. " + e.getMessage());
	    	    				}
	    	    	        	schedule.remove(job);
	    	    				
	    	    				// Immediately reschedule if it's possible. Let the execution manager
	    	    				// handle any duplicate runs.
	    	    				if (runningJob.updateTime()) {
	    	    					schedule.add(runningJob);
	    	    				}
    	    					saveSchedule();
	    	    			}
	    	    			else {
	    	    				// wait until job run
	    	    				long millisWait = Math.max(0, job.getScheduledExecution().getMillis() - (new DateTime()).getMillis());
	    	    				try {
	    							this.wait(Math.min(millisWait, TIMEOUT_MS));
	    						} catch (InterruptedException e) {
	    							// interruption should occur when items are added or removed from the queue.
	    						}
	    	    			}
	    	    		}
        			}
        			catch (Exception e) {
        				logger.error("Unexpected exception has been thrown in scheduler", e);
        			}
        			catch (Throwable e) {
        				logger.error("Unexpected throwable has been thrown in scheduler", e);
        			}
        		}
        	}
        }
        
        /**
         * Class to sort the schedule based on time.
         * 
         * @author Richard Park
         */
        private class ScheduleComparator implements Comparator<ScheduledJob>{
    		@Override
    		public int compare(ScheduledJob arg0, ScheduledJob arg1) {
    			DateTime first = arg1.getScheduledExecution();
    			DateTime second = arg0.getScheduledExecution();
    			
    			if (first.isEqual(second)) {
    				return 0;
    			}
    			else if (first.isBefore(second)) {
    				return 1;
    			}
    			
    			return -1;
    		}	
        }
    }
}