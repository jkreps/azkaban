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

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.common.utils.Props;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.monitor.MonitorInterface.JobState;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.model.ExecutionModel;
import azkaban.monitor.model.ExecutionModelImpl;
import azkaban.monitor.model.JobExecutionModel;
import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

/**
 * Class to test execution model functionality.
 * @author donpazel
 *
 */
public class ExecutionModelTest {   
    /**
     * Logger used for this file.
     */
    private static final Logger LOG = Logger.getLogger(ExecutionModelTest.class);
    
    /**
     * Setup test method.
     * @throws Exception
     *     Exception thrown
     */
    @Before
    public void setUp() throws Exception { }
    
    /**
     * Teardown test method.
     * @throws Exception
     *       Exception thown
     */
    @After
    public void tearDown() throws Exception { }
    
    /**
     * Test for workflow of a single job.
     * @throws Exception
     *     Exception thrown
     */
    @Test
    public void testWorkflowSingleJob() throws Exception {
        String wfId = "23";
        String rootJobName = "rootJob";
        long schedTime = DateTimeUtils.currentTimeMillis();
        long increment = 1 * 1000;
        long incCount = 0;
        MonitorImpl aImpl = EasyMock.createStrictMock(MonitorImpl.class);
        
        ExecutionModel eModel = new ExecutionModelImpl(aImpl);
        
        LOG.info("Scheduling a workflow");
        eModel.scheduleWorkflow(rootJobName, schedTime  + (incCount++) * increment );
        
        NativeWorkflowClassStats nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        
        LOG.info("Starting a workflow");
        eModel.startWorkflow(rootJobName, wfId, schedTime + (incCount++) * increment);
        
        nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowStarted());
        
        // Start a fictitious job.
        String job1Name = "Job1";
        ProcessJob job1 = EasyMock.createStrictMock(ProcessJob.class);  
        Props job1Props = new Props();
        job1Props.put("azkaban.flow.id", wfId);
        
        EasyMock.expect(job1.getProps()).andReturn(job1Props).anyTimes();
        EasyMock.expect(job1.getId()).andReturn(job1Name).anyTimes();
        EasyMock.replay(job1);
        
        LOG.info("Starting a workflow job");
        eModel.startWorkflowJob(schedTime + (incCount++) * increment, job1);
        NativeJobClassStats job1Stats = eModel.getJobClassStatsById(job1Name);
        Assert.assertNotNull(job1Stats);
        Assert.assertEquals(1, job1Stats.getNumTimesJobStarted());
                       
        LOG.info("Ending a workflow job");
        eModel.endWorkflowJob(schedTime + (incCount++) * increment, job1, JobState.SUCCESSFUL);
        job1Stats = eModel.getJobClassStatsById(job1Name);
        Assert.assertNotNull(job1Stats);
        Assert.assertEquals(1, job1Stats.getNumTimesJobStarted());
        Assert.assertEquals(1, job1Stats.getNumTimesJobSuccessful());
        
        LOG.info("Ending a workflow");
        eModel.endWorkflow(wfId, schedTime + (incCount++) * increment, MonitorInterface.WorkflowState.SUCCESSFUL);
        
        nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowStarted());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowSuccessful());
        
        Map<String, NativeWorkflowClassStats> allWfStats = eModel.getAllWorkflowClassStats();
        List<String> allWfNames = eModel.getWorkflowClassIds();
        Assert.assertNotNull(allWfStats);
        Assert.assertEquals(allWfStats.size(), allWfNames.size());
        Assert.assertEquals(allWfNames.size(), eModel.getNumberOfWorkflows());
        Assert.assertEquals(1, eModel.getNumberOfCompletedWorkflows());
        
        NativeGlobalStats gs = eModel.getGlobalStatsCopy();
        Assert.assertEquals(1, gs.getTotalJobsStarted());
        Assert.assertEquals(1, gs.getTotalJobsSuccessful());
        Assert.assertEquals(1, gs.getTotalWorkflowsStarted());
        Assert.assertEquals(1, gs.getTotalWorkflowsSuccessful());
        
        List<String> jobIds = eModel.getJobClassIds();
        Assert.assertNotNull(jobIds);
        Assert.assertEquals(1, jobIds.size());
        Assert.assertEquals(job1Name, jobIds.get(0));
        
        NativeJobClassStats nativeJob = eModel.getJobClassStatsById(jobIds.get(0));
        Assert.assertNotNull(nativeJob);
        Assert.assertEquals(1, nativeJob.getNumTimesJobStarted());
        Assert.assertEquals(1, nativeJob.getNumTimesJobSuccessful());
        
        Map<String, NativeJobClassStats> allJobStats = eModel.getAllJobClassStats();
        Assert.assertNotNull(allJobStats);
        Assert.assertEquals(allJobStats.size(), jobIds.size());
        
        List<WorkflowExecutionModel> allWfModels = eModel.getCompletedWorkflowModels(schedTime + (incCount++) * increment, true);
        Assert.assertNotNull(allWfModels);
        Assert.assertEquals(1, allWfModels.size());
        WorkflowExecutionModel wfExecModel = allWfModels.get(0);
        Assert.assertNotNull(wfExecModel);
        long wfRunTime = wfExecModel.getExecutionTime();
        Assert.assertEquals(3000, wfRunTime);
        long wfElapsedTime = wfExecModel.getElapsedTime();
        Assert.assertEquals(4000, wfElapsedTime);
        long wfPendingTime = wfExecModel.getPendingTime();
        Assert.assertEquals(1000, wfPendingTime);
        Assert.assertTrue(wfExecModel.isCompleted());
        List<String> wfJobNames = wfExecModel.getWorkflowJobNames();
        Assert.assertEquals(1, wfJobNames.size());
        List<JobExecutionModel> wfJobExecs = wfExecModel.getWorkflowJobExecutions();
        Assert.assertNotNull(wfJobExecs);
        Assert.assertEquals(1, wfJobExecs.size());
        JobExecutionModel wfJobModel = wfJobExecs.get(0);
        Assert.assertNotNull(wfJobModel);
        Assert.assertTrue(wfJobModel.isCompleted());
        long jobExecRunTime = wfJobModel.getExecutionTime();
        Assert.assertEquals(1000, jobExecRunTime);
        Assert.assertEquals(wfExecModel, wfJobModel.getWorkflowModel());   
           
        eModel.clearCompletedWorkflows();
        allWfModels = eModel.getCompletedWorkflowModels(true);
        Assert.assertNotNull(allWfModels);
        Assert.assertEquals(0, allWfModels.size());

        LOG.info("End testWorkflowSingleJob");
    }
    
    /**
     * Test with multiple workflows.
     * @throws Exception
     *     Exception thrown
     */
    @Test
    public void testWorkflowMultiJob() throws Exception {
        String wfId = "32";
        String rootJobName = "rootJob";
        long schedTime = DateTimeUtils.currentTimeMillis();
        long increment = 1 * 1000;
        long incCount = 0;
        MonitorImpl aImpl = EasyMock.createStrictMock(MonitorImpl.class);
        
        ExecutionModel eModel = new ExecutionModelImpl(aImpl);
        
        LOG.info("Scheduling a workflow");
        eModel.scheduleWorkflow(rootJobName, schedTime + (incCount++) * increment);
        
        NativeWorkflowClassStats nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        
        LOG.info("Starting a workflow");
        eModel.startWorkflow(rootJobName, wfId, schedTime + (incCount++) * increment);
        
        nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowStarted());
        
        for (int i = 1; i <= 10; ++i) {
            String jobName = "Job" + i;
            ProcessJob job = EasyMock.createStrictMock(ProcessJob.class);  
            Props jobProps = new Props();
            jobProps.put("azkaban.flow.id", wfId);
            
            EasyMock.expect(job.getProps()).andReturn(jobProps).anyTimes();
            EasyMock.expect(job.getId()).andReturn(jobName).anyTimes();
            EasyMock.replay(job);
            
            LOG.info("Starting a workflow job");
            eModel.startWorkflowJob(schedTime + (incCount++) * increment, job);
            NativeJobClassStats jobStats = eModel.getJobClassStatsById(jobName);
            Assert.assertNotNull(jobStats);
            Assert.assertEquals(1, jobStats.getNumTimesJobStarted());
                           
            LOG.info("Ending a workflow job");
            JobState jState = JobState.SUCCESSFUL;
            if (i == 5) {
                jState = JobState.FAILED;
            } else if (i == 7) {
                jState = JobState.CANCELED;
            }
            eModel.endWorkflowJob(schedTime + (incCount++) * increment, job, jState);
            jobStats = eModel.getJobClassStatsById(jobName);
            Assert.assertNotNull(jobStats);
            Assert.assertEquals(1, jobStats.getNumTimesJobStarted());
            if (i == 5) {
                Assert.assertEquals(1, jobStats.getNumTimesJobFailed());
            } else if (i == 7) {
                Assert.assertEquals(1, jobStats.getNumTimesJobCanceled());
            } else {
                Assert.assertEquals(1, jobStats.getNumTimesJobSuccessful()); 
            }
        }
           
        LOG.info("Ending a workflow");
        eModel.endWorkflow(wfId, 
                           schedTime + (incCount++) * increment, 
                           MonitorInterface.WorkflowState.SUCCESSFUL);
        
        nativeWf = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(nativeWf);
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowScheduled());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowStarted());
        Assert.assertEquals(1, nativeWf.getNumTimesWorkflowSuccessful());
        
        NativeGlobalStats nativeGs = eModel.getGlobalStatsCopy();
        Assert.assertNotNull(nativeGs);
        Assert.assertEquals(10, nativeGs.getTotalJobsStarted());
        Assert.assertEquals(8, nativeGs.getTotalJobsSuccessful());
        Assert.assertEquals(1, nativeGs.getTotalJobsFailed());
        Assert.assertEquals(1, nativeGs.getTotalJobsCanceled());
        
        
        LOG.info("End testWorkflowMultiJob");
    }
    
    /**
     * Test multiple workflows and job classes.
     * @throws Exception
     *     Exception thrown
     */
    @Test
    public void testMultiWorkflowMultiJob() throws Exception {
        String rootJobName = "rootJob";
        long schedTime = DateTimeUtils.currentTimeMillis();
        long increment = 1 * 1000;
        MonitorImpl aImpl = EasyMock.createStrictMock(MonitorImpl.class);
        
        ExecutionModel eModel = new ExecutionModelImpl(aImpl);
        
        long totalWfTime = 0;
        long totalWfPendingTime = 0;
        for (int i = 58; i < 68; ++i) {
            String wfId = Integer.toString(i);
            LOG.info("Scheduling workflow " + i);
            long wfPendingStartTime = schedTime;
            eModel.scheduleWorkflow(rootJobName, schedTime);
            schedTime += increment;
            
            LOG.info("Starting a workflow");
            totalWfPendingTime += schedTime - wfPendingStartTime;
            long wfStartTime = schedTime;
            eModel.startWorkflow(rootJobName, wfId, schedTime);
            schedTime += increment;
            
            for (int j = 1; j <= 10; ++j) {
                String jobName = "Job" + j;
                ProcessJob job = EasyMock.createStrictMock(ProcessJob.class);  
                Props jobProps = new Props();
                jobProps.put("azkaban.flow.id", wfId);
                
                EasyMock.expect(job.getProps()).andReturn(jobProps).anyTimes();
                EasyMock.expect(job.getId()).andReturn(jobName).anyTimes();
                EasyMock.replay(job);
                
                LOG.info("Starting a workflow job");
                eModel.startWorkflowJob(schedTime, job);
                schedTime += increment  + Math.random() * 10.0;
                               
                LOG.info("Ending a workflow job");
                JobState jState = JobState.SUCCESSFUL;
                if (j == 5) {
                    jState = JobState.FAILED;
                } else if (j == 7) {
                    jState = JobState.CANCELED;
                }
                eModel.endWorkflowJob(schedTime, job, jState);
                schedTime += increment;
            }
                    
            LOG.info("Ending workflow " + i);
            WorkflowState wState = WorkflowState.SUCCESSFUL;
            if (i == 58) {
                wState = WorkflowState.FAILED;
            } else if (i == 59) {
                wState = WorkflowState.CANCELED;
            }
            eModel.endWorkflow(wfId, 
                               schedTime, 
                               wState);
            if (i != 58 && i != 59) {
                totalWfTime += (schedTime - wfStartTime);
            }
            schedTime += increment;
        }
        
        NativeJobClassStats job5Stats = eModel.getJobClassStatsById("Job5");
        Assert.assertNotNull(job5Stats);
        Assert.assertEquals(10, job5Stats.getNumTimesJobStarted());
        Assert.assertEquals(10, job5Stats.getNumTimesJobFailed());
        Assert.assertEquals(1000, job5Stats.getAvgJobFailedTime(), 20.0);
        Assert.assertEquals(5.0, job5Stats.getStdJobFailedTime(), 5.0);
        
        NativeJobClassStats job7Stats = eModel.getJobClassStatsById("Job7");
        Assert.assertNotNull(job7Stats);
        Assert.assertEquals(10, job7Stats.getNumTimesJobStarted());
        Assert.assertEquals(10, job7Stats.getNumTimesJobCanceled());
        Assert.assertEquals(1000, job7Stats.getAvgJobCanceledTime(), 20.0);
        Assert.assertEquals(5.0, job7Stats.getStdJobCanceledTime(), 5.0);
        
        NativeJobClassStats job9Stats = eModel.getJobClassStatsById("Job9");
        Assert.assertNotNull(job9Stats);
        Assert.assertEquals(10, job9Stats.getNumTimesJobStarted());
        Assert.assertEquals(10, job9Stats.getNumTimesJobSuccessful());
        Assert.assertEquals(1000, job9Stats.getAvgJobRunTime(), 20.0);
        Assert.assertEquals(5.0, job9Stats.getStdJobRunTime(), 5.0);
        
        List<String> wfClassList = eModel.getWorkflowClassIds();
        Assert.assertNotNull(wfClassList);
        Assert.assertEquals(1, wfClassList.size());
        Assert.assertEquals(rootJobName, wfClassList.get(0));
        NativeWorkflowClassStats wfStats = eModel.getWorkflowClassStatsById(rootJobName);
        Assert.assertNotNull(wfStats);
        Assert.assertEquals(10, wfStats.getNumTimesWorkflowScheduled());
        Assert.assertEquals(10, wfStats.getNumTimesWorkflowStarted());
        Assert.assertEquals(8, wfStats.getNumTimesWorkflowSuccessful());
        Assert.assertEquals(1, wfStats.getNumTimesWorkflowFailed());
        Assert.assertEquals(1, wfStats.getNumTimesWorkflowCanceled());
        Assert.assertEquals(wfStats.getAvgWorkflowRunTime(), totalWfTime / 8.0, 1.0);
        Assert.assertEquals(wfStats.getAvgWorkflowPendingTime(), totalWfPendingTime / 10.0, 1.0);
        
        LOG.info("End testMultiWorkflowMultiJob");
    }
    
    /**
     * Test multiple workflows, testing workflow models.
     * @throws Exception
     *     Exception thrown
     */
    @Test
    public void testMultiWorkflowSameWfid() throws Exception {       
        String wfId = "89";
        String rootJobName = "rootJob";
        long schedTime = DateTimeUtils.currentTimeMillis();
        long increment = 1 * 1000;
        long incCount = 0;
        MonitorImpl aImpl = EasyMock.createStrictMock(MonitorImpl.class);
        
        ExecutionModel eModel = new ExecutionModelImpl(aImpl);
        
        LOG.info("Scheduling a workflow");
        eModel.scheduleWorkflow(rootJobName, schedTime + (incCount++) * increment);
        
        LOG.info("Starting a workflow");
        eModel.startWorkflow(rootJobName, wfId, schedTime + (incCount++) * increment);
         
        LOG.info("Ending a workflow");
        eModel.endWorkflow(wfId, 
                           schedTime + (incCount++) * increment, 
                           MonitorInterface.WorkflowState.SUCCESSFUL);
        
        List<String> wfModelNames = eModel.getWorkflowClassIds();
        Assert.assertEquals(1, wfModelNames.size());
        List<WorkflowExecutionModel> wfModelsPass1 = eModel.getCompletedWorkflowModels(schedTime + (incCount++) * increment, false);
        Assert.assertEquals(1, wfModelsPass1.size());
        WorkflowExecutionModel olderWfModer = wfModelsPass1.get(0);
        Assert.assertNotNull(olderWfModer);
        
        // Restart the workflow       
        LOG.info("Starting a workflow");
        eModel.startWorkflow(rootJobName, wfId, schedTime + (incCount++) * increment);
        
        LOG.info("Ending a workflow");
        eModel.endWorkflow(wfId, 
                           schedTime + (incCount++) * increment, 
                           MonitorInterface.WorkflowState.SUCCESSFUL);
        
        
        // Explore how the workflow model structure.
        List<WorkflowExecutionModel> wfModelsPass2 = eModel.getCompletedWorkflowModels(schedTime + (incCount++) * increment, false);
        Assert.assertEquals(1, wfModelsPass2.size());
        WorkflowExecutionModel newerWfModel = wfModelsPass2.get(0);
        Assert.assertNotNull(newerWfModel);
        Assert.assertEquals(olderWfModer, newerWfModel.getOlderExecutionModel());        
    }
    
    /**
     * Test clearing out completed workflows.
     * @throws Exception
     *     Exception thrown.
     */
    @Test
    public void testClearWorkflowSameWfid() throws Exception { 
        String wfId = "89";
        String rootJobName = "rootJob";
        long schedTime = DateTimeUtils.currentTimeMillis() - 5 * 1000;
        long increment = 1 * 1000;
        long incCount = 0;
        MonitorImpl aImpl = EasyMock.createStrictMock(MonitorImpl.class);
        
        ExecutionModel eModel = new ExecutionModelImpl(aImpl);
        
        LOG.info("Scheduling a workflow");
        eModel.scheduleWorkflow(rootJobName, schedTime);
        
        LOG.info("Starting a workflow");
        eModel.startWorkflow(rootJobName, wfId, schedTime + (incCount++) * increment);
         
        LOG.info("Ending a workflow");
        eModel.endWorkflow(wfId, 
                           schedTime + (incCount++) * increment, 
                           MonitorInterface.WorkflowState.SUCCESSFUL);
        
        List<WorkflowExecutionModel> wfModelsPass1 = eModel.getCompletedWorkflowModels(schedTime, false);
        Assert.assertEquals(0, wfModelsPass1.size());
        eModel.clearCompletedWorkflows(schedTime);
        long numWorkflowsLeft = eModel.getNumberOfWorkflows();
        Assert.assertEquals(1, numWorkflowsLeft);
        
        List<WorkflowExecutionModel> wfModelsPass2 = eModel.getCompletedWorkflowModels(false);
        Assert.assertEquals(1, wfModelsPass2.size());
        eModel.clearCompletedWorkflows();
        numWorkflowsLeft = eModel.getNumberOfWorkflows();
        Assert.assertEquals(0, numWorkflowsLeft);
    }

}
