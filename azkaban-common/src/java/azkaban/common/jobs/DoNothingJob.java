package azkaban.common.jobs;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

/**
 * A placeholder job that does nothing. Used for defining barriers for
 * convenient dependency management
 * 
 * E.g. this job can be used solely as a way of grouping dependencies
 * 
 * @author jkreps
 * 
 */
public class DoNothingJob extends AbstractJob {

    public DoNothingJob(String name, Props props) {
        super(name);
    }

    public void run() {
    // does nothing, just a placeholder for dependencies
    }

}
