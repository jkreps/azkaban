package azkaban.jobs;

import java.io.File;
import java.util.concurrent.TimeUnit;

import azkaban.app.JobDescriptor;
import azkaban.common.jobs.AbstractJob;
import azkaban.util.process.AzkabanProcess;
import azkaban.util.process.AzkabanProcessBuilder;

/**
 * A revised process-based job
 * 
 * @author jkreps
 *
 */
public abstract class AbstractProcessJob extends AbstractJob {
    
    public static final String ENV_PREFIX = "env.";
    public static final String COMMAND = "command";
    public static final String WORKING_DIR = "working.dir";
    
    private static final long KILL_TIME_MS = 5000;

    private final AzkabanProcessBuilder builder;
    private final JobDescriptor descriptor;
    private volatile AzkabanProcess process;
    
    public AbstractProcessJob(String[] command, JobDescriptor desc) {
        super(desc.getId());
        this.descriptor = desc;
        String cwd = descriptor.getProps().getString(WORKING_DIR, new File(descriptor.getFullPath()).getParent());
        this.builder = new AzkabanProcessBuilder(command).setEnv(descriptor.getProps().getMapByPrefix(ENV_PREFIX)).setWorkingDir(cwd);
    }

    public void run() throws Exception {
        info("Command: " + builder.getCommandString());
        info("Environment: " + builder.getEnv());
        info("Working directory: " + builder.getWorkingDir());
        
        this.process = builder.build();
        try {
            this.process.run();
        } finally {
            this.process = null;
        }
    }
    
    /**
     * @return The job descriptor for this job
     */
    protected JobDescriptor getDescriptor() {
        return this.descriptor;
    }
    
    /**
     * This gives access to the process builder used to construct the process. An overriding class can use this to 
     * add to the command being executed.
     */
    protected AzkabanProcessBuilder getBuilder() {
        return this.builder;
    }
    
    @Override
    public void cancel() throws InterruptedException {
        if(process == null)
            throw new IllegalStateException("Not started.");
        boolean killed = process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
        if(!killed) {
            warn("Kill with signal TERM failed. Killing with KILL signal.");
            process.hardKill();
        }
    }
    
    @Override
    public double getProgress() {
        return process != null && process.isComplete()? 1.0 : 0.0;
    }
    
}
