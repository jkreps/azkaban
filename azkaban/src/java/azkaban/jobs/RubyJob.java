package azkaban.jobs;

import azkaban.app.JobDescriptor;

import com.google.common.collect.ImmutableSet;


public class RubyJob extends LongArgJob {

    private static final String RUBY_BINARY_KEY = "ruby";
    private static final String SCRIPT_KEY = "script";

    public RubyJob(JobDescriptor desc) {
        super(new String[]{desc.getProps().getString(RUBY_BINARY_KEY, "ruby"), 
                           desc.getProps().getString(SCRIPT_KEY)}, 
              desc, 
              ImmutableSet.of(RUBY_BINARY_KEY, SCRIPT_KEY, JobDescriptor.JOB_TYPE));
    }

   
    
}
