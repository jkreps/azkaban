/*
 * Copyright (C) 2011 Adconion, Inc
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
package azkaban.jobcontrol.impl.jobs;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.common.utils.Props;
import azkaban.flow.FlowCallback;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.jobs.Status;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.monitor.MonitorImpl;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;
import azkaban.monitor.MonitorListener;
import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * @author <a href="mailto:iulukaya@adconion.com">Ibrahim Ulukaya</a>
 * Tests to check canceling Retrying jobs 
 */
public class RetryingJobTest implements MonitorListener{
    private static Logger logger = Logger.getLogger(RetryingJobTest.class);
    private volatile JobManager jobManager;

    private volatile AtomicBoolean assertionViolated;
    private volatile String reason;
    private volatile MonitorImpl monitor;
    
    private final String wfId = "25";
    private final String rootJobName = "MyJob";

    private int specificJobClassNotifications;
    
    @BeforeClass
    public static void init() throws Exception {
    }

    @AfterClass
    public static void cleanup() {
    }

    @Before
    public void setUp()
    {
        jobManager = EasyMock.createMock(JobManager.class);

        assertionViolated = new AtomicBoolean(false);
        reason = "Default Reason";

        monitor = (MonitorImpl)MonitorImpl.getMonitor();
        specificJobClassNotifications = 0;     
    }

    @After
    public void tearDown()
    {
        monitor.deregisterJobClassNotification(this, rootJobName);
        MonitorImpl.unsetMonitor();
    }

    @Test
    public void testCancelRetryJobs() throws Throwable
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        monitor.registerJobClassNotification(this, rootJobName);
        
        ProcessJob job = null;
        RetryingJob retryJob = null;
        JobDescriptor descriptor = null;
        Props props = null;
        
        descriptor = EasyMock.createMock(JobDescriptor.class);
        
        props = new Props();
        props.put(ProcessJob.WORKING_DIR, ".");
        props.put("type", "command");
        props.put(ProcessJob.COMMAND, "sleep 10");
        props.put("azkaban.flow.id", wfId);

        descriptor = new JobDescriptor(rootJobName, null, ".", props, null);

        job = new ProcessJob(descriptor);
        retryJob = new RetryingJob(job, 3, 500);

        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow(wfId, rootJobName, jobManager);

        EasyMock.expect(jobManager.loadJob(rootJobName, props, true)).andReturn(retryJob).once();

        monitor.workflowEvent(null, 
                System.currentTimeMillis(),              
                WorkflowAction.SCHEDULE_WORKFLOW, 
                WorkflowState.NOP,
                wfId);

        monitor.workflowEvent(wfId, 
                System.currentTimeMillis(),
                WorkflowAction.START_WORKFLOW, 
                WorkflowState.NOP,
                rootJobName);

        EasyMock.replay(jobManager);
        Assert.assertEquals(Status.READY, executableFlow.getStatus());

        executableFlow.execute(
                props,
                new FlowCallback()
                {
                    @Override
                    public void progressMade() 
                    {
                    }

                    @Override
                    public void completed(Status status) 
                    {
                        completionLatch.countDown();
                        // Lazy failure is used since this doesn't run in the main thread
                        if (Status.FAILED != status) {
                            assertionViolated.set(true);
                            reason = String.format("In executableFlow Callback: status[%s] != Status.FAILED", status);
                        }
                    }
                });
        completionLatch.await(2, TimeUnit.SECONDS);
        executableFlow.cancel();
        monitor.workflowEvent(wfId, 
                System.currentTimeMillis(),
                WorkflowAction.END_WORKFLOW, 
                WorkflowState.FAILED,
                rootJobName);
        completionLatch.await(12, TimeUnit.SECONDS);
        List<WorkflowExecutionModel> wfmodels = monitor.getCompletedWorkflowModels(false);
        logger.info("num wf models " + wfmodels.size());
        WorkflowExecutionModel wfModel = wfmodels.get(0);
        List<String> names = wfModel.getWorkflowJobNames();
        Assert.assertNotNull(names);
        //If the underlying job retries, names will have size 3.
        Assert.assertEquals(2, names.size());
        Assert.assertEquals(rootJobName, names.get(0));
        Assert.assertEquals(WorkflowState.FAILED, wfModel.getCompletionState());
        Assert.assertEquals(4, specificJobClassNotifications);
        Assert.assertFalse(reason, assertionViolated.get());
        EasyMock.verify(jobManager);
    }
    
    @Override
    public void onGlobalNotify(GlobalNotificationType type, ClassStats statsObject) {
    }

    @Override
    public void onJobNotify(NativeJobClassStats jobStats) {
        logger.info("Received job Class Notification for: " + jobStats.getJobClassName());
        specificJobClassNotifications++;
    }

    @Override
    public void onWorkflowNotify(NativeWorkflowClassStats wfStats) {
    }
}
