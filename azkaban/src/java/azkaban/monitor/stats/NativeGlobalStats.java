/*
 * Copyright 2010 Adconion, Inc
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
 */package azkaban.monitor.stats;


/**
 * Base data-version of GlobalStats comprising all data fields and getters.
 * Includes constructor for building clone of current values.  Used to acquire
 * stable version of actual NativeGlobalStats, for user.
 * This class is not thread safe.
 *
 */
public class NativeGlobalStats extends ClassStats {
    /**
     * Number of workflows scheduled.
     */
    protected long totalWorkflowsScheduled;
    
    /**
     * Number of workflows started.
     */
    protected long totalWorkflowsStarted;
    
    /**
     * Number of workflows that completed successfully.
     */
    protected long totalWorkflowsSuccessful;
    
    /**
     * Number of workflows that failed.
     */
    protected long totalWorkflowsFailed;
    
    /**
     * Number of workflows that were canceled.
     */
    protected long totalWorkflowsCanceled;
    
    /**
     * Number of currently pending workflows.
     */
    protected long totalWorkflowsPending; 
    
    /**
     * Number of jobs started.
     */
    protected long totalJobsStarted;
    
    /** 
     * Number of jobs that terminated successfully.
     */
    protected long totalJobsSuccessful;
    
    /**
     * Number of jobs that failed.
     */
    protected long totalJobsFailed;
    
    /**
     * Number of jobs that were canceled.
     */
    protected long totalJobsCanceled;
    
    /**
     * Highest workflow id started.
     */
    protected long highFlowId;     
    
    /**
     * Time of most recent workflow started (to millisec.)
     */
    protected long startTime;      
    
    /**
     * How long total monitoring has been occurring (to millisec.)
     */
    protected long timeMonitoring;   
    
    /**
     * Primary constructor for the global statistics object.
     */
    public NativeGlobalStats() {
        super();
    }
    
    /**
     * Copy constructor to construct duplicate based on what is in the master copy.
     * Includes only the critical statistics.
     * @param ngs 
     *            the global stats being copied.
     */
    public NativeGlobalStats(NativeGlobalStats ngs) {
        super();  // Do not pass monitor to copies.
        synchronized (ngs) {
            this.totalWorkflowsScheduled = ngs.totalWorkflowsScheduled;
            this.totalWorkflowsStarted = ngs.totalWorkflowsStarted;
            this.totalWorkflowsSuccessful = ngs.totalWorkflowsSuccessful;
            this.totalWorkflowsFailed = ngs.totalWorkflowsFailed;
            this.totalWorkflowsCanceled = ngs.totalWorkflowsCanceled;
            this.totalWorkflowsPending = ngs.totalWorkflowsPending;
            this.totalJobsStarted = ngs.totalJobsStarted;
            this.totalJobsSuccessful = ngs.totalJobsSuccessful;
            this.totalJobsFailed = ngs.totalJobsFailed;
            this.totalJobsCanceled = ngs.totalJobsCanceled;
            this.highFlowId = ngs.highFlowId;
            this.startTime = ngs.startTime;
            this.timeMonitoring = System.currentTimeMillis() - ngs.startTime;
        }
    }  
    
    /**
     * getter
     * @return the number of workflows scheduled.
     */
    public final long getTotalWorkflowsScheduled() {
        return totalWorkflowsScheduled;
    }
    
    /**
     * getter
     * @return the number of workflows started.
     */
    public final long getTotalWorkflowsStarted() {
        return totalWorkflowsStarted;
    }
    
    /**
     * getter
     * @return the number of workflows successful.
     */
    public final long getTotalWorkflowsSuccessful() {
        return totalWorkflowsSuccessful;
    }
    
    /**
     * getter
     * @return the number of workflows failed.
     */
    public final long getTotalWorkflowsFailed() {
        return totalWorkflowsFailed;
    }
    
    /**
     * getter
     * @return the number of workflows canceled.
     */
    public final long getTotalWorkflowsCanceled() {
        return totalWorkflowsCanceled;
    }
    
    /**
     * getter
     * @return the number of workflows pending.
     */
    public final long getTotalWorkflowsPending() {
        return totalWorkflowsPending;
    }
    
    /**
     * getter
     * @return the number of jobs started.
     */
    public final long getTotalJobsStarted() {
        return totalJobsStarted;
    }
    
    /**
     * getter
     * @return the number of successful jobs.
     */
    public final long getTotalJobsSuccessful() {
        return totalJobsSuccessful;
    }
    
    /**
     * getter
     * @return the number of failed jobs.
     */
    public final long getTotalJobsFailed() {
        return totalJobsFailed;
    }
    
    /**
     * getter
     * @return the number of jobs canceled.
     */
    public final long getTotalJobsCanceled() {
        return totalJobsCanceled;
    }
    
    /**
     * getter
     * @return the highest flow id recorded.
     */
    public final long getHighFlowId() {
        return highFlowId;
    }
    
    /**
     * getter
     * @return the amount of time in ms monitoring.
     */
    public final long getTimeMonitoring() {
        return timeMonitoring;
    }
    
    /**
     * getter
     * @return the epoch time in ms when monitoring started.
     */
    public final long getStartTime() {
        return startTime;
    }
}
