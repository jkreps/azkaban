/*
 * Copyright 2010 Adconion, Inc
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
 */package azkaban.monitor;

import java.util.Calendar;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.common.utils.Props;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.jobcontrol.impl.jobs.RetryingJob;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.JobAction;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * This is a variety of tests for the monitoring system, focusing on retry jobs
 * and the notification mechanism.
 *
 */
public class MonitorImplTest implements MonitorListener {
    private static Logger logger = Logger.getLogger(MonitorImplTest.class);
    
    private MonitorInterface external;
    private MonitorInternalInterface internal;
    
    private int globalNotifications;
    private int anyWorkflowClassNotifications;
    private int anyJobClassNotifications;
    private int specificWorkflowClassNotifications;
    private int specificJobClassNotifications;
    
    @Before
    public void setUp() throws Exception
    {       
        MonitorImpl impl = (MonitorImpl)MonitorImpl.getMonitor();
        external = impl;
        internal = impl;
    }
    
    @After
    public void tearDown() throws Exception {
        MonitorImpl.unsetMonitor();
    }
    
    @Test
    public void testRetries() throws Exception {
        String wfId = "25";
        String rootJobName = "MyJob";
        Props props = new Props();
        Props replyProps = new Props();
        props.put("azkaban.flow.id", wfId);
        
        // This is the inner job for the retry job.
        ProcessJob rootJob = EasyMock.createStrictMock(ProcessJob.class);      
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.replay(rootJob);
        
        // Create the retrying job;
        RetryingJob retryingJob = new RetryingJob(rootJob, 4, 2);
        
        // Do in order
        // Not that if core logic changes, this choreography needs 
        // updating.
        EasyMock.reset(rootJob);
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        rootJob.run();
        EasyMock.expectLastCall().andThrow(new RuntimeException());
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.expect(rootJob.isCanceled()).andReturn(false);
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        rootJob.run();
        EasyMock.expectLastCall().andThrow(new RuntimeException());       
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.expect(rootJob.isCanceled()).andReturn(false);
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        rootJob.run(); 
        EasyMock.expect(rootJob.getJobGeneratedProperties()).andReturn(replyProps).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        
        EasyMock.replay(rootJob);
        
        // We need to fields some events related to a hypothetical workflow and
        // initiating the retry job.
        MonitorImpl.getInternalMonitorInterface().workflowEvent(null, System.currentTimeMillis(), 
                WorkflowAction.SCHEDULE_WORKFLOW, WorkflowState.NOP, rootJobName); 
        MonitorImpl.getInternalMonitorInterface().workflowEvent(wfId, System.currentTimeMillis(), 
                WorkflowAction.START_WORKFLOW, WorkflowState.NOP, rootJobName);  
        MonitorImpl.getInternalMonitorInterface().jobEvent( 
                retryingJob,
                System.currentTimeMillis(), 
                JobAction.START_WORKFLOW_JOB, JobState.NOP);
        
        //  Run the retry job    
        retryingJob.run();
        
        // Close out the retry job and workflow with events.
        MonitorImpl.getInternalMonitorInterface().jobEvent( 
                retryingJob,
                System.currentTimeMillis(), 
                JobAction.END_WORKFLOW_JOB, JobState.SUCCESSFUL);
        MonitorImpl.getInternalMonitorInterface().workflowEvent(wfId, System.currentTimeMillis(), 
                WorkflowAction.END_WORKFLOW, WorkflowState.SUCCESSFUL, null);
        
       // Get the job stats and test a few values.
       NativeJobClassStats jobStats = external.getJobClassStatsByName(rootJobName);
       Assert.assertNotNull(jobStats);
       
       Assert.assertEquals(1, jobStats.getNumRetryJobStarts());
       Assert.assertEquals(3, jobStats.getNumJobTries());
       Assert.assertEquals(2, jobStats.getNumTimesJobFailed());
       Assert.assertEquals(1, jobStats.getNumTimesJobSuccessful());
       
       logger.info("End testRetries");
    }
    
    @Test
    public void testResourceThrottle() throws Exception {
        String wfId = "26";
        String rootJobName = "ResourceJob";
        long delayTime = 3000L;
        // Do a second Job
        ProcessJob rootJob = EasyMock.createStrictMock(ProcessJob.class);
        Props props = new Props();
        Props replyProps = new Props();
        props.put("azkaban.flow.id", wfId);
        TimeDelayJobLock tjl = new TimeDelayJobLock(delayTime);
        
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.replay(rootJob);
        
        ResourceThrottledJob resourceJob = new ResourceThrottledJob(rootJob, tjl);
        
        // Do in order
        // Not that if core logic changes, this choreography needs 
        // updating.
        EasyMock.reset(rootJob);
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        rootJob.run();
        EasyMock.expect(rootJob.getJobGeneratedProperties()).andReturn(replyProps).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.replay(rootJob);
        
        // We need to fields some events related to a hypothetical workflow and
        // initiating the retry job.
        MonitorImpl.getInternalMonitorInterface().workflowEvent(null, System.currentTimeMillis(), 
                WorkflowAction.SCHEDULE_WORKFLOW, WorkflowState.NOP, rootJobName); 
        MonitorImpl.getInternalMonitorInterface().workflowEvent(wfId, System.currentTimeMillis(), 
                WorkflowAction.START_WORKFLOW, WorkflowState.NOP, rootJobName);  
        MonitorImpl.getInternalMonitorInterface().jobEvent( 
                resourceJob,
                System.currentTimeMillis(), 
                JobAction.START_WORKFLOW_JOB, JobState.NOP);
        
        resourceJob.run();       
        
        // Close out the retry job and workflow with events.
        MonitorImpl.getInternalMonitorInterface().jobEvent( 
                resourceJob,
                System.currentTimeMillis(), 
                JobAction.END_WORKFLOW_JOB, JobState.SUCCESSFUL);
        MonitorImpl.getInternalMonitorInterface().workflowEvent(wfId, System.currentTimeMillis(), 
                WorkflowAction.END_WORKFLOW, WorkflowState.SUCCESSFUL, null);
        
        // Get the job stats and test the wait lock value.
        NativeJobClassStats jobStats = external.getJobClassStatsByName(rootJobName);
        Assert.assertNotNull(jobStats);
        // account for non-determinism of thread.sleep().
        long epsilon = 10;
        Assert.assertTrue(String.format(
                "Resource throttled wait time should be >= %d but was %f",
                delayTime, jobStats.getTotalResourceThrottledWaitTime()),
                jobStats.getTotalResourceThrottledWaitTime() >= (delayTime - epsilon));

        
        logger.info("End testResourceThrottle");
    }
    
    @Test
    public void testNotifications() throws Exception { 
        String wfId = "21";
        String rootJobName = "root";
        Props props = new Props();
        props.put("azkaban.flow.id", wfId);
        
        ProcessJob rootJob = EasyMock.createStrictMock(ProcessJob.class);
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.expect(rootJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(rootJob.getId()).andReturn(rootJobName).anyTimes();
        EasyMock.replay(rootJob);
        
        Calendar baseCalendar = Calendar.getInstance();
        baseCalendar.setTimeInMillis(0L);   // clear out ms field
        baseCalendar.set(2010, 9, 25, 9, 25, 0);
        long baseTimeMS = baseCalendar.getTimeInMillis();
            
        external.registerGlobalNotification(this, GlobalNotificationType.GLOBAL_STATS_CHANGE);
        external.registerGlobalNotification(this, GlobalNotificationType.ANY_WORKFLOW_CLASS_STATS_CHANGE);
        external.registerGlobalNotification(this, GlobalNotificationType.ANY_JOB_CLASS_STATS_CHANGE);
        external.registerWorkflowClassNotification(this, rootJob.getId());
        external.registerJobClassNotification(this, rootJob.getId());
           
        Calendar scheduleTime = Calendar.getInstance();
        scheduleTime.setTimeInMillis(baseTimeMS);
        
        internal.workflowEvent(null, 
                scheduleTime.getTimeInMillis(),              
                WorkflowAction.SCHEDULE_WORKFLOW, 
                WorkflowState.NOP,
                rootJobName);
        
        Calendar startTime = Calendar.getInstance();
        startTime.setTimeInMillis(baseTimeMS + 1000);
        internal.workflowEvent(wfId,
                startTime.getTimeInMillis(),
                WorkflowAction.START_WORKFLOW,
                WorkflowState.NOP,
                rootJobName);
        
        Calendar jobStartTime = Calendar.getInstance();
        jobStartTime.setTimeInMillis(baseTimeMS + 2000);
        internal.jobEvent(
                rootJob,
                jobStartTime.getTimeInMillis(),
                JobAction.START_WORKFLOW_JOB,
                JobState.NOP);
        
        Calendar jobEndTime = Calendar.getInstance();
        jobEndTime.setTimeInMillis(baseTimeMS + 3000);
        internal.jobEvent(
                rootJob,
                jobEndTime.getTimeInMillis(),
                JobAction.END_WORKFLOW_JOB,
                JobState.SUCCESSFUL);
                
        Calendar endTime = Calendar.getInstance();
        endTime.setTimeInMillis(baseTimeMS + 4000);
        internal.workflowEvent(wfId,
                endTime.getTimeInMillis(),
                WorkflowAction.END_WORKFLOW,
                WorkflowState.SUCCESSFUL,
                null);
        
        Assert.assertEquals(5, globalNotifications);  
        Assert.assertEquals(3, anyWorkflowClassNotifications); 
        Assert.assertEquals(2, anyJobClassNotifications); 
        Assert.assertEquals(3, specificWorkflowClassNotifications); 
        Assert.assertEquals(2, specificJobClassNotifications); 
        
        NativeGlobalStats globalStats = external.getGlobalAzkabanStats();
        Assert.assertEquals(1, globalStats.getTotalWorkflowsStarted());
        Assert.assertEquals(1, globalStats.getTotalJobsStarted());
        
        NativeWorkflowClassStats wfClassStats = external.getWorkflowClassStatsByRootJobName(rootJob.getId());
        Assert.assertNotNull(wfClassStats);
        Assert.assertEquals(wfClassStats.getAvgWorkflowRunTime(), 
                            (double)(endTime.getTimeInMillis() - startTime.getTimeInMillis()));
        
        NativeJobClassStats jobClassStats = external.getJobClassStatsByName(rootJob.getId());
        Assert.assertNotNull(jobClassStats);
        Assert.assertEquals(jobClassStats.getAvgJobRunTime(), 
                            (double)(jobEndTime.getTimeInMillis() - jobStartTime.getTimeInMillis()));
        
        external.deregisterNotifications(this);
        List<MonitorListener> notifiers = ((MonitorImpl)external).getAllNotifiers();
        Assert.assertNotNull(notifiers);
        Assert.assertEquals(0, notifiers.size());
      
        logger.info("End testNotifications");
    }
    
    @Override
    public void onGlobalNotify(GlobalNotificationType type, ClassStats statsObject) {
        switch(type) {
        case GLOBAL_STATS_CHANGE:
            logger.info("Received Global Notification");
            Assert.assertNotNull(statsObject);
            Assert.assertTrue(statsObject instanceof NativeGlobalStats);
            globalNotifications++;
            break;
        case ANY_WORKFLOW_CLASS_STATS_CHANGE:
            logger.info("Received Any Workflow Class Notification");
            Assert.assertNotNull(statsObject);
            Assert.assertTrue(statsObject instanceof NativeWorkflowClassStats);
            anyWorkflowClassNotifications++;
            break;
        case ANY_JOB_CLASS_STATS_CHANGE:
            logger.info("Received Any Job Class Notification");
            Assert.assertNotNull(statsObject);
            Assert.assertTrue(statsObject instanceof NativeJobClassStats);
            anyJobClassNotifications++;
            break;
        }
    }

    @Override
    public void onJobNotify(NativeJobClassStats jobStats) {
        logger.info("Received job Class Notification for: " + jobStats.getJobClassName());
        specificJobClassNotifications++;
    }

    @Override
    public void onWorkflowNotify(NativeWorkflowClassStats wfStats) {
        logger.info("Received wf Class Notification for: " + wfStats.getWorkflowRootName());
        specificWorkflowClassNotifications++;
    }
}
