package azkaban.common.jobs;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

/**
 * A Job that just sleeps for the given amount of time
 * 
 * @author jkreps
 * 
 */
public class SleepyJob extends AbstractJob {

    private final long _sleepTimeMs;

    public SleepyJob(String name, Props p) {
        super(name);
        this._sleepTimeMs = p.getLong("sleep.time.ms", Long.MAX_VALUE);
    }

    public void run() {
        info("Sleeping for " + _sleepTimeMs + " ms.");
        try {
            Thread.sleep(_sleepTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
