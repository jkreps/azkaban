package azkaban.jobcontrol.impl.jobs;

import org.apache.log4j.Logger;

import azkaban.common.jobs.DelegatingJob;
import azkaban.common.jobs.Job;
import azkaban.common.jobs.JobFailedException;

public class RetryingJob extends DelegatingJob {

    private final Logger _logger;
    private final int _retries;
    private final long _retryBackoff;

    public RetryingJob(Job innerJob, int retries, long retryBackoff) {
        super(innerJob);
        _logger = Logger.getLogger(innerJob.getId());
        _retries = retries;
        _retryBackoff = retryBackoff;
    }

    @Override
    public void run() {
        for(int tries = 0; tries <= _retries; tries++) {
            // helpful logging info
            if(tries > 0) {
                if(_retryBackoff > 0) {
                    _logger.info("Chillaxing for " + _retryBackoff + " ms until next job retry.");
                    try {
                        Thread.sleep(_retryBackoff);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                _logger.info("Retrying failed job '" + getInnerJob().getId() + " for attempt "
                             + (tries + 1));
            }

            try {
                getInnerJob().run();
                return;
            } catch(Exception e) {
                _logger.error("Job '" + getInnerJob().getId() + " failed attempt " + (tries + 1), e);
                String sadness = "";
                for(int i = 0; i < tries + 1; i++)
                    sadness += ":-( ";
                _logger.info(sadness);
            }
        }

        // if we get here it means we haven't succeded (otherwise we would have
        // returned)
        throw new JobFailedException(_retries + " run attempt" + (_retries > 1 ? "s" : "")
                                     + " failed.");
    }

}
