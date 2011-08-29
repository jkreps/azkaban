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
package azkaban.monitor;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import azkaban.jobs.AbstractProcessJob;
import azkaban.jobs.Status;
import azkaban.jobs.builtin.PythonJob;
import azkaban.jobs.builtin.Utils;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;
import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * @author ibrahimulukaya
 * Tests to check LongArgJob events in Monitor
 */
public class LongArgJobMonitorTest implements MonitorListener{
    private static Logger logger = Logger.getLogger(MonitorImplTest.class);
    private volatile JobManager jobManager;

    private volatile AtomicBoolean assertionViolated;
    private volatile String reason;
    private volatile MonitorImpl monitor;
    private static Map<String, Throwable> emptyExceptions;
    
    private final String wfId = "25";
    private final String rootJobName = "MyJob";

    private int specificJobClassNotifications;
    
    private static final String scriptContentSuccess = 
          "#!/usr/bin/python \n" +
          "import sys \n" +
          "print sys.argv";
      
     private static final String scriptContentFail = 
          "#!/usr/bin/python \n" +
          "import sys \n" +
          "print sys.argv \n" +
          "sys.exit(1)";
      
     private static String scriptFileSuccess ;
     private static String scriptFileFail;

    @BeforeClass
    public static void init() throws Exception {
      emptyExceptions = new HashMap<String, Throwable>();
      long time = (new Date()).getTime();
      scriptFileSuccess = "/tmp/azkaban_python" + time + ".py";
      scriptFileFail = "/tmp/prop_print" + time + ".py";
      // dump script file
      try {
        Utils.dumpFile(scriptFileSuccess, scriptContentSuccess);
        Utils.dumpFile(scriptFileFail, scriptContentFail);
       }
      catch (IOException e) {
        e.printStackTrace(System.err);
        Assert.fail("error in creating script file:" + e.getLocalizedMessage());
      }          
    }

    @AfterClass
    public static void cleanup() {
       // remove the input file and error input file
       Utils.removeFile(scriptFileSuccess);
       Utils.removeFile(scriptFileFail);
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
    public void testLongArgSuccess() throws Throwable
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        monitor.registerJobClassNotification(this, rootJobName);
        
        PythonJob pythonJob = null;
        JobDescriptor descriptor = null;
        Props props = null;
        
        descriptor = EasyMock.createMock(JobDescriptor.class);
        
        props = new Props();
        props.put("input_date", "20110505");
        props.put(AbstractProcessJob.WORKING_DIR, ".");
        props.put("type", "python");
        props.put("script", scriptFileSuccess);
        props.put("output_dir", "/data/report-${input_date}");
        props.put("azkaban.flow.id", wfId);
        
        EasyMock.expect(descriptor.getId()).andReturn(rootJobName).times(1);
        EasyMock.expect(descriptor.getProps()).andReturn(props).times(3);
        EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);

        EasyMock.replay(descriptor);

        pythonJob = new PythonJob(descriptor);
        EasyMock.verify(descriptor);

        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow(wfId, rootJobName, jobManager);

        EasyMock.expect(jobManager.loadJob(rootJobName, props, true)).andReturn(pythonJob).once();

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
                        if (Status.SUCCEEDED != status) {
                            assertionViolated.set(true);
                            reason = String.format("In executableFlow Callback: status[%s] != Status.SUCCEEDED", status);
                        }
                    }
                });

        monitor.workflowEvent(wfId, 
                System.currentTimeMillis(),
                WorkflowAction.END_WORKFLOW, 
                WorkflowState.SUCCESSFUL,
                rootJobName);

        completionLatch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.SUCCEEDED, executableFlow.getStatus());
        Assert.assertEquals(emptyExceptions, executableFlow.getExceptions());

        List<WorkflowExecutionModel> wfmodels = monitor.getCompletedWorkflowModels(false);
        logger.info("num wf models " + wfmodels.size());
        WorkflowExecutionModel wfModel = wfmodels.get(0);
        List<String> names = wfModel.getWorkflowJobNames();
        Assert.assertNotNull(names);
        Assert.assertEquals(1, names.size());
        Assert.assertEquals(rootJobName, names.get(0));
        Assert.assertEquals(WorkflowState.SUCCESSFUL, wfModel.getCompletionState());
        Assert.assertEquals(2, specificJobClassNotifications);
        Assert.assertFalse(reason, assertionViolated.get());
        EasyMock.verify(jobManager);

    }

    @Test
    public void testLongArgFail() throws Throwable
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        monitor.registerJobClassNotification(this, rootJobName);
        
        PythonJob pythonJob = null;
        JobDescriptor descriptor = null;
        Props props = null;
        
        descriptor = EasyMock.createMock(JobDescriptor.class);
        
        props = new Props();
        props.put("input_date", "20110505");
        props.put(AbstractProcessJob.WORKING_DIR, ".");
        props.put("type", "python");
        props.put("script", scriptFileFail);
        props.put("output_dir", "/data/report-${input_date}");
        props.put("azkaban.flow.id", wfId);
        
        EasyMock.expect(descriptor.getId()).andReturn(rootJobName).times(1);
        EasyMock.expect(descriptor.getProps()).andReturn(props).times(3);
        EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);

        EasyMock.replay(descriptor);

        pythonJob = new PythonJob(descriptor);
        EasyMock.verify(descriptor);

        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow(wfId, rootJobName, jobManager);
        EasyMock.expect(jobManager.loadJob(rootJobName, props, true)).andReturn(pythonJob).once();

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
        Assert.assertEquals(emptyExceptions, executableFlow.getExceptions());

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

        monitor.workflowEvent(wfId, 
                System.currentTimeMillis(),
                WorkflowAction.END_WORKFLOW,
                WorkflowState.FAILED,
                rootJobName);

        completionLatch.await(4000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.FAILED, executableFlow.getStatus());

        List<WorkflowExecutionModel> wfmodels = monitor.getCompletedWorkflowModels(false);
        logger.info("num wf models " + wfmodels.size());
        WorkflowExecutionModel wfModel = wfmodels.get(0);
        List<String> names = wfModel.getWorkflowJobNames();
        Assert.assertNotNull(names);
        Assert.assertEquals(1, names.size());
        Assert.assertEquals(rootJobName, names.get(0));
        Assert.assertEquals(WorkflowState.FAILED, wfModel.getCompletionState());       
        Assert.assertEquals(2, specificJobClassNotifications);
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
