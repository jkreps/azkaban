package azkaban.jobs;

import java.util.HashSet;
import java.util.Set;

import azkaban.app.JobDescriptor;
import azkaban.common.utils.Props;
import azkaban.util.process.AzkabanProcessBuilder;


/**
 * A job that passes all the job properties as command line arguments in "long" format, 
 * e.g. --key1 value1 --key2 value2 ...
 * 
 * @author jkreps
 *
 */
public abstract class LongArgJob extends AbstractProcessJob {
    
    public LongArgJob(String[] command, JobDescriptor desc) {
        this(command, desc, new HashSet<String>(0));
    }
    
    public LongArgJob(String[] command, JobDescriptor desc, Set<String> suppressedKeys) {
        super(command, desc);
        appendProps(suppressedKeys);
    }

    private void appendProps(Set<String> suppressed) {
        AzkabanProcessBuilder builder = this.getBuilder();
        Props props = this.getDescriptor().getProps();
        for(String key: props.keySet())
            if(!suppressed.contains(key))
                builder.addArg("--" + key, props.get(key));
    }
    
}
