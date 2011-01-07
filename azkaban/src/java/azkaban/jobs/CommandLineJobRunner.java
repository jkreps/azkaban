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

package azkaban.jobs;

import azkaban.app.JobManager;
import azkaban.app.JobWrappingFactory;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowCallback;
import azkaban.flow.FlowManager;
import azkaban.flow.RefreshableFlowManager;
import azkaban.jobs.Status;
import azkaban.jobcontrol.impl.jobs.locks.NamedPermitManager;
import azkaban.jobcontrol.impl.jobs.locks.ReadWriteLockManager;
import azkaban.jobs.builtin.JavaJob;
import azkaban.jobs.builtin.JavaProcessJob;
import azkaban.jobs.builtin.PigProcessJob;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.serialization.DefaultExecutableFlowSerializer;
import azkaban.serialization.ExecutableFlowSerializer;
import azkaban.serialization.FlowExecutionSerializer;
import azkaban.serialization.de.DefaultExecutableFlowDeserializer;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.serialization.de.FlowExecutionDeserializer;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

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
        Props overrideProps = new Props(null);
        for(String override: options.valuesOf(overrideOpt)) {
            String[] pieces = override.split("=");
            if(pieces.length != 2)
                Utils.croak("Invalid property override: '" + override
                            + "', properties must be in the form key=value", 1);
            overrideProps.put(pieces[0], pieces[1]);
        }

        NamedPermitManager permitManager = new NamedPermitManager();
        permitManager.createNamedPermit("default", cl.getNumWorkPermits());

        JobWrappingFactory factory = new JobWrappingFactory(permitManager,
                                                            new ReadWriteLockManager(),
                                                            cl.getLogDir().getAbsolutePath(),
                                                            "java",
                                                            ImmutableMap.<String, Class<? extends Job>>of("java", JavaJob.class,
                                                                                                          "command", ProcessJob.class,
                                                                                                          "javaprocess", JavaProcessJob.class,
                                                                                                          "pig", PigProcessJob.class));

        JobManager jobManager = new JobManager(factory,
                                               cl.getLogDir().getAbsolutePath(),
                                               cl.getDefaultProps(),
                                               cl.getJobDirs(),
                                               cl.getClassloader());

        File executionsStorageFile = new File(".");
        if(!executionsStorageFile.exists()) {
            executionsStorageFile.mkdirs();
        }

        long lastId = 0;
        for(File file: executionsStorageFile.listFiles()) {
            final String filename = file.getName();
            if(filename.endsWith(".json")) {
                try {
                    lastId = Math.max(lastId,
                                      Long.parseLong(filename.substring(0, filename.length() - 5)));
                } catch(NumberFormatException e) {}
            }
        }

        final ExecutableFlowSerializer flowSerializer = new DefaultExecutableFlowSerializer();
        final ExecutableFlowDeserializer flowDeserializer = new DefaultExecutableFlowDeserializer(jobManager, factory);
        FlowExecutionSerializer flowExecutionSerializer = new FlowExecutionSerializer(flowSerializer);
        FlowExecutionDeserializer flowExecutionDeserializer = new FlowExecutionDeserializer(flowDeserializer);


        FlowManager allFlows = new RefreshableFlowManager(jobManager,
                                                          flowExecutionSerializer,
                                                          flowExecutionDeserializer,
                                                          executionsStorageFile,
                                                          lastId);
        jobManager.setFlowManager(allFlows);

        final CountDownLatch countDown = new CountDownLatch(jobNames.size());

        for(String jobName: jobNames) {
            try {
                final ExecutableFlow flowToRun = allFlows.createNewExecutableFlow(jobName);

                if (flowToRun == null) {
                    System.out.printf("Job[%s] is unknown.  Not running.%n", jobName);

                    countDown.countDown();
                    continue;
                }
                else {
                    System.out.println("Running " + jobName);
                }

                if (ignoreDeps) {
                    for (ExecutableFlow flow : flowToRun.getChildren()) {
                        flow.markCompleted();
                    }
                }


                flowToRun.execute(
                        overrideProps,
                        new FlowCallback()
                        {
                            @Override
                            public void progressMade()
                            {
                            }

                            @Override
                            public void completed(Status status)
                            {
                                if (status == Status.FAILED) {
                                    System.out.printf("Job failed.%n");
                                    final Map<String, Throwable> exceptions = flowToRun.getExceptions();

                                    if (exceptions == null || exceptions.isEmpty()) {
                                        System.out.println("flowToRun.getExceptions() was null/empty when it should not have been.  Please notify the Azkaban developers.");
                                    }

                                    int errorNum = 1;
                                    for (Entry<String, Throwable> entry: exceptions.entrySet()) {
                                        String name = entry.getKey();
                                        System.err.println(errorNum + ".  "  + name + "\n");
                                        entry.getValue().printStackTrace(System.err);
                                        System.err.println("\n\n");
                                        errorNum ++;
                                    }
                                        
                                }

                                countDown.countDown();
                            }
                        });
            } catch(Exception e) {
                System.out.println("Failed to run job '" + jobName + "':");
                e.printStackTrace();
            }
        }

        countDown.await();
    }
}
