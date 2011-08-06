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
 * Base data-version of JobClassStats comprising all data fields and getters.
 * Includes constructor for building clone of current values.  Used to acquire
 * stable version of actual NativeJobClassStats, for user.
 * This class is not thread safe.
 */
public class NativeJobClassStats extends ClassStats {
    /**
     * Name of the job class.
     */
    protected String jobClassName;
    
    /**
     * Number of times this job was started.
     */
    protected long numTimesJobStarted;
    
    /**
     * Number of times this job ended successfully.
     */
    protected long numTimesJobSuccessful;
    
    /**
     * Number of times this job ended in failure.
     */
    protected long numTimesJobFailed;
    
    /**
     * Number of times this job was canceled.
     */
    protected long numTimesJobCanceled;
    
    /**
     * Average runtime for this job, as a successful run.
     */
    protected double avgJobRunTime;
    
    /**
     * Standard deviation of job runtime, as a successful run.
     */
    protected double stdJobRunTime;

    /**
     * Average runtime for this job, as failed job.
     */
    protected double avgJobFailedTime;
    
    /**
     * Standard deviation of job, as a failed job.
     */
    protected double stdJobFailedTime;
    
    /**
     * Average runtime for this job, as a canceled job.
     */
    protected double avgJobCanceledTime;
    
    /**
     * Standard deviation of job, as a canceled job.
     */
    protected double stdJobCanceledTime;
    
    /**
     * Most recent start time for this job class.
     */
    protected long lastTimeJobStarted;
    
    /**
     * Indicates if this is a retry job.
     */
    protected boolean retryJob;
    
    /**
     * Indicates if this is a resource throttled job.
     */
    protected boolean resourceThrottledJob;
    
    /**
     * Indicates if this is a logging job.
     */
    protected boolean loggingJob;
    
    /**
     * Number of times the retry job (that is, the job that does retries) starts.
     */
    protected long numRetryJobStarts;
    
    /**
     * Number of times the retry job itself was successful.
     */
    protected long numRetryJobSuccessful;
    
    /**
     * Number of times the retry job itself failed.
     */
    protected long numRetryJobFailures;
    
    /**
     * Number of times the retry job itself was canceled.
     */
    protected long numRetryJobCanceled;
    
    /**
     * The total number of job tries - should be equal to num started.
     */
    protected long numJobTries;
    
    /**
     * Average number of retries per retry job run.
     */
    protected double avgNumJobTries;
    
    /**
     * Average run time for the retry job itself [includes its retries.]
     */
    protected double avgRetryJobRunTime;
    
    /**
     * Standard deviation of the retry job runtime itself.
     */
    protected double stdRetryJobRunTime;
    
    /**
     * If this is a resource throttled jobs, this the the total lock waiting time (in ms).
     */
    protected double totalResourceThrottledWaitTime;
    
    /**
     * This is the average resource throttled waiting time (in ms).
     */
    protected double avgResourceThrottledWaitTime;
    
    /**
     * Standard deviation resource throttled waiting time (in ms).
     */
    protected double stdResourceThrottledWaitTime;
    
    /**
     * This is the total number of resouce throttled starts.
     */
    protected long numResourceThrottledStarted;
    
    /**
     * Number of stats as a logging job.
     */
    protected long numLoggingJobStarts;
    
    
    /**
     * Primary constructor for JobClassStats.
     */
    public NativeJobClassStats() {
        super();
    }
    
    /**
     * Copy constructor to construct duplicate based on what is in the master copy.
     * Includes only the critical statistics.
     * @param njcs 
     *            NativeJobClassStats being copied.
     */
    public NativeJobClassStats(NativeJobClassStats njcs) {
        super();     // Do not pass monitor to copies.
        synchronized (njcs) {
            this.jobClassName = njcs.jobClassName;
            this.numTimesJobStarted = njcs.numTimesJobStarted;
            this.numTimesJobSuccessful = njcs.numTimesJobSuccessful;
            this.numTimesJobFailed = njcs.numTimesJobFailed;
            this.numTimesJobCanceled = njcs.numTimesJobCanceled;
            this.avgJobRunTime = njcs.avgJobRunTime;
            this.stdJobRunTime = njcs.stdJobRunTime;
            this.avgJobFailedTime = njcs.avgJobFailedTime;
            this.stdJobFailedTime = njcs.stdJobFailedTime;
            this.avgJobCanceledTime = njcs.avgJobCanceledTime;
            this.stdJobCanceledTime = njcs.stdJobCanceledTime;
            this.lastTimeJobStarted = njcs.lastTimeJobStarted;
            this.retryJob = njcs.retryJob;
            this.resourceThrottledJob = njcs.resourceThrottledJob;
            this.loggingJob = njcs.loggingJob;
            
            this.numRetryJobStarts = njcs.numRetryJobStarts;
            this.numRetryJobSuccessful = njcs.numRetryJobSuccessful;
            this.numRetryJobFailures = njcs.numRetryJobFailures;
            this.numRetryJobCanceled = njcs.numRetryJobCanceled;
            this.numJobTries = njcs.numJobTries;
            this.avgNumJobTries = njcs.avgNumJobTries;
            this.avgRetryJobRunTime = njcs.avgRetryJobRunTime;
            this.stdRetryJobRunTime = njcs.stdRetryJobRunTime;
            this.totalResourceThrottledWaitTime = njcs.totalResourceThrottledWaitTime;
            this.avgResourceThrottledWaitTime = njcs.avgResourceThrottledWaitTime;
            this.stdResourceThrottledWaitTime = njcs.stdResourceThrottledWaitTime;
            this.numResourceThrottledStarted = njcs.numResourceThrottledStarted;
            this.numLoggingJobStarts = njcs.numLoggingJobStarts;
        }
    }
     
    @Override
    public final Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    
    /**
     * getter
     * @return job class name
     */
    public final String getJobClassName() {
        return jobClassName;
    }
    
    /**
     * getter
     * @return number of times job class started.
     */
    public final long getNumTimesJobStarted() {
        return numTimesJobStarted;
    }

    /**
     * getter
     * @return number of times job class failed.
     */
    public final long getNumTimesJobFailed() {
        return numTimesJobFailed;
    }
    
    /**
     * getter
     * @return number of times job class successful.
     */
    public final long getNumTimesJobSuccessful() {
        return numTimesJobSuccessful;
    }

    /**
     * getter
     * @return number of times job class canceled.
     */
    public final long getNumTimesJobCanceled() {
        return numTimesJobCanceled;
    }

    /**
     * getter
     * @return average job run time.
     */
    public final double getAvgJobRunTime() {
        return avgJobRunTime;
    }

    /**
     * getter
     * @return stand. dev. of run time.
     */
    public final double getStdJobRunTime() {
        return stdJobRunTime;
    }

    /**
     * getter
     * @return last teim (epoch ms) job of this class started.
     */
    public final long getLastTimeJobStarted() {
        return lastTimeJobStarted;
    }
    
    /**
     * getter
     * @return true if retrying job.
     */
    public final boolean isRetryJob() {
        return retryJob;
    }
    
    /**
     * getter
     * @return true if logging job.
     */
    public final boolean isLoggingJob() {
        return loggingJob;
    }
    
    /**
     * getter
     * @return true if is resource throttled job.
     */
    public final boolean isResourceThrottledJob() {
        return resourceThrottledJob;
    }
    
    /**
     * getter
     * @return number of times job retried [if trying job].
     */
    public final long getNumRetryJobStarts() {
        return numRetryJobStarts;
    }
    
    /**
     * getter
     * @return number of time retry job was successful.
     */
    public final long getNumRetryJobSuccessful() {
        return numRetryJobSuccessful;
    }

    /**
     * getter
     * @return number of times retry job failed.
     */
    public final long getNumRetryJobFailures() {
        return numRetryJobFailures;
    }
    
    /**
     * getter
     * @return number of time retry job was cancelled.
     */
    public final long getNumRetryJobCanceled() {
        return numRetryJobCanceled;
    }

    /**
     * getter
     * @return number of times job was retried.
     */
    public final long getNumJobTries() {
        return numJobTries;
    }
    
    /**
     * getter
     * @return average number of times inner job retried.
     */
    public final double getAvgNumJobTries() {
        return avgNumJobTries;
    }

    /**
     * getter
     * @return average runtime (ms) retry job ran.
     */
    public final double getAvgRetryJobRunTime() {
        return avgRetryJobRunTime;
    }
    
    /**
     * getter
     * @return stand. dev. of retry job runtime.
     */
    public final double getStdRetryJobRunTime() {
        return stdRetryJobRunTime;
    }
    
    /**
     * getter
     * @return total resource lock wait time in ms. [if resource throttled job]
     */
    public final double getTotalResourceThrottledWaitTime() {
        return totalResourceThrottledWaitTime;
    }

    /**
     * getter
     * @return average resource wait time [if resource throttled job]
     */
    public final double getAvgResourceThrottledWaitTime() {
        return avgResourceThrottledWaitTime;
    }
    
    /**
     * getter
     * @return stand. dev. of resource wait time [if resource throttled job]
     */
    public final double getStdResourceThrottledWaitTime() {
        return stdResourceThrottledWaitTime;
    }
    
    /**
     * getter
     * @return num times resource throttled job started.
     */
    public final long getNumResourceThrottledStart() {
        return numResourceThrottledStarted;
    }
    
    /**
     * getter
     * @return num times logging job starts.
     */
    public final long getNumLoggingJobStarts() {
        return numLoggingJobStarts;
    }
    
    /**
     * getter
     * @return num time resource throttled job started.
     */
    public final long getNumResourceThrottledStarted() {
        return numResourceThrottledStarted;
    }
    
    /**
     * getter
     * @return average job failed time (ms).
     */
    public final double getAvgJobFailedTime() {
        return avgJobFailedTime;
    }

    /**
     * getter
     * @return stand. dev. fo job fail time (ms).
     */
    public final double getStdJobFailedTime() {
        return stdJobFailedTime;
    }

    /**
     * getter
     * @return average run time on canceled jobs (ms).
     */
    public final double getAvgJobCanceledTime() {
        return avgJobCanceledTime;
    }

    /**
     * getter
     * @return stand. dev. for run time on canceled jobs (ms).
     */
    public final double getStdJobCanceledTime() {
        return stdJobCanceledTime;
    }
}
