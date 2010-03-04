package azkaban.jobcontrol.impl.jobs;

import org.apache.log4j.Logger;

import azkaban.jobcontrol.impl.jobs.locks.JobLock;

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

    public ResourceThrottledJob(Job job, JobLock lock) {
        super(job);
        _jobLock = lock;
        this._logger = Logger.getLogger(job.getId());
    }

    /**
     * Wrapper that acquires needed permits, runs job, and then releases permits
     */
    @Override
    public void run() {
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
        try {
            getInnerJob().run();
        } finally {
            _jobLock.releaseLock();
        }
    }
}
