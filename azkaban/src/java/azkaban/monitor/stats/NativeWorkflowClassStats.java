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
 * Base data-version of WorkflowClassStats comprising all data fields and getters.
 * Includes constructor for building clone of current values.  Used to acquire
 * stable version of actual NativeWorkflowClassStats, for user.
 * This class is not thread safe.
 *
 */
public class NativeWorkflowClassStats extends ClassStats {
    /**
     * Name of the root job for this workflow.
     */
    protected String workflowRootName;
    
    /**
     * Number of times this workflow was scheduled.
     */
    protected long numTimesWorkflowScheduled;
    
    /**
     * Number of times this workflow was started.
     */
    protected long numTimesWorkflowStarted;
    
    /**
     * Number of times this workflow ended successfully.
     */
    protected long numTimesWorkflowSuccessful;
    
    /**
     * Number of times this workflow ended in failure.
     */
    protected long numTimesWorkflowFailed;
    
    /**
     * number of times this workflow was canceled.
     */
    protected long numTimesWorkflowCanceled;
    
    /**
     * Average runtime for this workflow successfulrun.  Strictly start to end,
     * does not include scheduled time.
     */
    protected double avgWorkflowRunTime;
    
    /**
     * Standard deviation for workflow successful run time.
     */
    protected double stdWorkflowRunTime;
    
    /**
     * Average runtime for workflows that failed.
     */
    protected double avgWorkflowFailedTime;

    /**
     * Standard deviation of runtime for workflows that failed.
     */
    protected double stdWorkflowFailedTime;   

    /**
     * Average runtime for workflows that were canceled.
     */
    protected double avgWorkflowCanceledTime;

    /**
     * Standard deviateion of runtime for workflows that are canceled.
     */
    protected double stdWorkflowCanceledTime;

    /**
     * Average time workflow spend in pending state.
     */
    protected double avgWorkflowPendingTime;
    
    /**
     * Standard deviation for workflow pending.
     */
    protected double stdWorkflowPendingTime;
    
    /**
     * Most recent time this workflow was started.
     */
    protected long lastWorkflowTimeStarted;

    /**
     * Constructor used to build the workflow class
     */
    public NativeWorkflowClassStats() {
        super();
    }
    
    /**
     * Copy constructor to construct duplicate based on what is in the master copy.
     * Includes only the critical statistics.
     * @param nwcs 
     *            NativeWorkflowClassStats being copied.
     */
    public NativeWorkflowClassStats(NativeWorkflowClassStats nwcs) {
        super();   // Do not pass monitor to copies.
        synchronized (nwcs) {
            this.workflowRootName = nwcs.workflowRootName;
            this.numTimesWorkflowScheduled = nwcs.numTimesWorkflowScheduled;
            this.numTimesWorkflowStarted = nwcs.numTimesWorkflowStarted;
            this.numTimesWorkflowSuccessful = nwcs.numTimesWorkflowSuccessful;
            this.numTimesWorkflowFailed = nwcs.numTimesWorkflowFailed;
            this.numTimesWorkflowCanceled = nwcs.numTimesWorkflowCanceled;
            this.avgWorkflowRunTime = nwcs.avgWorkflowRunTime;
            this.stdWorkflowRunTime = nwcs.stdWorkflowRunTime;
            this.avgWorkflowFailedTime = nwcs.avgWorkflowFailedTime;
            this.stdWorkflowFailedTime = nwcs.stdWorkflowFailedTime;
            this.avgWorkflowCanceledTime = nwcs.stdWorkflowCanceledTime;
            this.avgWorkflowPendingTime = nwcs.avgWorkflowPendingTime;
            this.stdWorkflowPendingTime = nwcs.stdWorkflowPendingTime;
            this.lastWorkflowTimeStarted = nwcs.lastWorkflowTimeStarted;
        }
    }
    
    @Override
    public final Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    
    /**
     * getter
     * @return name of root job.
     */
    public final String getWorkflowRootName() {
        return workflowRootName;
    }
    
    /**
     * getter
     * @return number of times workflow scheduled.
     */
    public final long getNumTimesWorkflowScheduled() {
        return numTimesWorkflowScheduled;
    }

    /**
     * getter
     * @return number of times workflow started.
     */
    public final long getNumTimesWorkflowStarted() {
        return numTimesWorkflowStarted;
    }

    /**
     * getter
     * @return number of times workflow was successful.
     */
    public final long getNumTimesWorkflowSuccessful() {
        return numTimesWorkflowSuccessful;
    }

    /**
     * getter
     * @return number of times workflow failed.
     */
    public final long getNumTimesWorkflowFailed() {
        return numTimesWorkflowFailed;
    }

    /**
     * getter
     * @return number of time workflow canceled.
     */
    public final long getNumTimesWorkflowCanceled() {
        return numTimesWorkflowCanceled;
    }

    /**
     * getter
     * @return average workflow run time in ms.
     */
    public final double getAvgWorkflowRunTime() {
        return avgWorkflowRunTime;
    }

    /**
     * getter
     * @return stand. dev. for workflow run time.
     */
    public final double getStdWorkflowRunTime() {
        return stdWorkflowRunTime;
    }

    /**
     * getter
     * @return last time workflow of this class was started.
     */
    public final long getLastWorkflowTimeStarted() {
        return lastWorkflowTimeStarted;
    }
    
    /**
     * getter
     * @return stand. dev. for pending time (ms).
     */
    public final double getStdWorkflowPendingTime() {
        return stdWorkflowPendingTime;
    }   
    
    /**
     * getter
     * @return average workflow pending time (scheduled but not started) ms.
     */
    public final double getAvgWorkflowPendingTime() {
        return avgWorkflowPendingTime;
    }
     
    /**
     * getter
     * @return average workflow runtime for failed workflows.
     */
    public final double getAvgWorkflowFailedTime() {
        return avgWorkflowFailedTime;
    }
    
    /**
     * getter
     * @return stand. dev. on runtime for failed workflows (ms).
     */
    public final double getStdWorkflowFailedTime() {
        return stdWorkflowFailedTime;
    }
    
    /**
     * getter
     * @return average canceled workflow runtime.
     */
    public final double getAvgWorkflowCanceledTime() {
        return avgWorkflowCanceledTime;
    }
    
    /**
     * getter
     * @return stand. dev on runtime of canceled workflows.
     */
    public final double getStdWorkflowCanceledTime() {
        return stdWorkflowCanceledTime;
    }
}
