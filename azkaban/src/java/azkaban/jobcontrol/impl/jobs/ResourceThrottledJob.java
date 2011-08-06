/*
 * Copyright 2010 LinkedIn, Inc
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
 */

package azkaban.jobcontrol.impl.jobs;

import org.apache.log4j.Logger;

import azkaban.jobcontrol.impl.jobs.locks.JobLock;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInternalInterface.JobAction;

import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;

/**
 * A wrapper that blocks the given job until the required number of permits
 * become available
 *
 * @author jkreps
 *
 */
public class ResourceThrottledJob extends DelegatingJob {

    private final JobLock _jobLock;
    private final Logger _logger;

    private final Object lock = new Object();
    private volatile boolean canceled = false;

    public ResourceThrottledJob(Job job, JobLock lock) {
        super(job);
        _jobLock = lock;
        this._logger = Logger.getLogger(job.getId());
    }

    /**
     * Wrapper that acquires needed permits, runs job, and then releases permits
     */
    @Override
    public void run() throws Exception
    {
        long start = System.currentTimeMillis();
        _logger.info("Attempting to acquire " + _jobLock + " at time " + start);
        try {
            _jobLock.acquireLock();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        long totalWait = System.currentTimeMillis() - start;
        _logger.info(_jobLock + " Time: " + totalWait + " ms.");
        
        MonitorImpl.getInternalMonitorInterface().workflowResourceThrottledJobEvent(this, totalWait);
        
        try {
            boolean shouldRunJob;
            synchronized(lock) {
                shouldRunJob = ! canceled;
            }

            MonitorImpl.getInternalMonitorInterface().jobEvent( 
                    getInnerJob(),
                    System.currentTimeMillis(),
                    JobAction.START_WORKFLOW_JOB,
                    JobState.NOP);
            
            if(shouldRunJob) {
                getInnerJob().run();
                
                MonitorImpl.getInternalMonitorInterface().jobEvent( 
                        getInnerJob(),
                        System.currentTimeMillis(),
                        JobAction.END_WORKFLOW_JOB,
                        JobState.SUCCESSFUL);
            }
            else {
                _logger.info("Job was canceled while waiting for lock.  Not running.");
                MonitorImpl.getInternalMonitorInterface().jobEvent( 
                        getInnerJob(),
                        System.currentTimeMillis(),
                        JobAction.END_WORKFLOW_JOB,
                        JobState.CANCELED);
            }
        } catch (Exception e) {
            MonitorImpl.getInternalMonitorInterface().jobEvent( 
                    getInnerJob(),
                    System.currentTimeMillis(),
                    JobAction.END_WORKFLOW_JOB,
                    JobState.FAILED);
            throw e;
        } finally {
            _jobLock.releaseLock();
        }
    }

    @Override
    public void cancel() throws Exception
    {
        synchronized (lock) {
            canceled = true;

            super.cancel();
        }
    }
    
    public synchronized boolean isCanceled() {
        return canceled;
    }
}
