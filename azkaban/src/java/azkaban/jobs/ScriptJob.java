package azkaban.jobs;

import com.google.common.collect.ImmutableSet;

import azkaban.app.JobDescriptor;

/**
 * A script job issues a command of the form
 *    [EXECUTABLE] [SCRIPT] --key1 val1 ... --key2 val2
 *   executable -- the interpretor command to execute
 *   script -- the script to pass in (requried)
 * 
 * @author jkreps
 *
 */
public class ScriptJob extends LongArgJob {

    private static final String DEFAULT_EXECUTABLE_KEY = "executable";
    private static final String SCRIPT_KEY = "script";
    
    public ScriptJob(JobDescriptor desc) {
        super(new String[] {desc.getProps().getString(DEFAULT_EXECUTABLE_KEY), desc.getProps().getString(SCRIPT_KEY)}, 
              desc, 
              ImmutableSet.of(DEFAULT_EXECUTABLE_KEY, SCRIPT_KEY, JobDescriptor.JOB_TYPE));
    }
 

}
