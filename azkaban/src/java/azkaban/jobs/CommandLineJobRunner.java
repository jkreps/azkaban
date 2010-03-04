package azkaban.jobs;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.app.Scheduler;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.FlowManager;
import azkaban.flow.Flows;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static java.util.Arrays.asList;

/**
 * Runs a job from the command line
 * 
 * The usage is
 * 
 * java azkaban.job.CommandLineJobRunner props-file prop_key=prop_val
 * 
 * Any argument that contains an '=' is assumed to be a property, all others are
 * assumed to be properties files for the job
 * 
 * The order of the properties files matters--in the case where both define a
 * property it will be read from the last file given.
 * 
 * @author jkreps
 * 
 */
public class CommandLineJobRunner {

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> overrideOpt = parser.acceptsAll(asList("o", "override"),
                                                           "An override property to be used instead of what is in the job")
                                               .withRequiredArg()
                                               .describedAs("key=val");
        String ignoreDepsOpt = "ignore-deps";
        parser.accepts(ignoreDepsOpt, "Run only the specified job, ignoring dependencies");
        AzkabanCommandLine cl = new AzkabanCommandLine(parser, args);

        String helpMessage = "USAGE: bin/run-job.sh [options] job_name...";
        OptionSet options = cl.getOptions();
        if(cl.hasHelp())
            cl.printHelpAndExit(helpMessage, System.out);

        List<String> jobNames = options.nonOptionArguments();
        if(jobNames.size() < 1)
            cl.printHelpAndExit(helpMessage, System.err);
                
        // parse override properties
        boolean ignoreDeps = options.has(ignoreDepsOpt);
        Props overrides = new Props(null);
        for(String override: options.valuesOf(overrideOpt)) {
            String[] pieces = override.split("=");
            if(pieces.length != 2)
                Utils.croak("Invalid property override: '" + override
                            + "', properties must be in the form key=value", 1);
            overrides.put(pieces[0], pieces[1]);
        }

        JobManager jobManager = new JobManager(cl.getLogDir().getAbsolutePath(),
                                               cl.getDefaultProps(),
                                               cl.getJobDirs(),
                                               cl.getClassloader(),
                                               cl.getNumWorkPermits());

/*
        FlowManager allFlows = new FlowManager();
        FlowManager rootFlows = new FlowManager();
        for (JobDescriptor rootDescriptor : jobManager.getRootJobDescriptors(jobManager.loadJobDescriptors())) {
            rootFlows.registerFlow(Flows.buildLegacyFlow(jobManager, allFlows, rootDescriptor));
        }
*/

        Scheduler scheduler = new Scheduler(jobManager,
//                                            rootFlows,
null,
                                            null,
                                            null,
                                            null,
                                            cl.getClassloader(),
                                            null,
                                            null,
                                            3);

        List<ScheduledFuture<?>> jobCompletionFutures = new ArrayList<ScheduledFuture<?>>();
        for(String jobName: jobNames) {
            try {
                System.out.println("Running " + jobName);
                Job theJob = jobManager.loadJob(jobName, overrides, ignoreDeps);
                jobCompletionFutures.add(scheduler.schedule(theJob.getId(),
                                                            new DateTime(),
                                                            ignoreDeps));
            } catch(Exception e) {
                System.out.println("Failed to run job '" + jobName + "':");
                e.printStackTrace();
            }
        }

        // wait for jobs to finish
        for(ScheduledFuture<?> future: jobCompletionFutures)
            future.get();
    }

}
