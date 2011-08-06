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

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.app.LoggingJob;
import azkaban.common.utils.Props;
import azkaban.jobcontrol.impl.jobs.ResourceThrottledJob;
import azkaban.jobcontrol.impl.jobs.RetryingJob;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.monitor.model.FlowDirectory;

public class FlowDirectoryTest {
    private static Logger logger = Logger.getLogger(FlowDirectoryTest.class);
    
    private FlowDirectory flowDirectory;
    
    @Before
    public void setUp() throws Exception { 
        flowDirectory = FlowDirectory.getFlowDirectory();
    }
    
    @After
    public void tearDown() throws Exception {
        FlowDirectory.unsetFlowDirectory();
    }
    
    @Test
    public void testFlowDirectory() throws Exception {
        String wfId = "26";
        String pJobName = "pjobname";
        Props props = new Props();
        props.put("azkaban.flow.id", wfId);
        
        ProcessJob pJob = EasyMock.createStrictMock(ProcessJob.class);
        ProcessJob p1Job = EasyMock.createStrictMock(ProcessJob.class);
        
        EasyMock.expect(pJob.getId()).andReturn(pJobName).anyTimes();
        EasyMock.expect(pJob.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(pJob.getId()).andReturn(pJobName).anyTimes();
        EasyMock.replay(pJob);
        
        EasyMock.expect(p1Job.getId()).andReturn(pJobName).anyTimes();
        EasyMock.expect(p1Job.getProps()).andReturn(props).anyTimes();
        EasyMock.expect(p1Job.getId()).andReturn(pJobName).anyTimes();
        EasyMock.replay(p1Job);
        
        RetryingJob  retryJob = new RetryingJob(pJob, 1, 1);
        ResourceThrottledJob rtJob = new ResourceThrottledJob(retryJob, new TimeDelayJobLock(500L));
        LoggingJob lJob = new LoggingJob("/Users/root", rtJob, "finiky");
        
        flowDirectory.mapJob(lJob);
        flowDirectory.mapJob(p1Job);
        
        Assert.assertEquals(retryJob, flowDirectory.getDelegatingJobParent(pJob));
        Assert.assertEquals(rtJob, flowDirectory.getDelegatingJobParent(retryJob));
        Assert.assertEquals(lJob, flowDirectory.getDelegatingJobParent(rtJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(pJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(pJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(retryJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(rtJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(lJob));
        Assert.assertEquals(wfId, flowDirectory.getFlowId(p1Job));
        
        flowDirectory.removeJobReference(lJob);
        Assert.assertNull(flowDirectory.getDelegatingJobParent(rtJob));
        Assert.assertNull(flowDirectory.getFlowId(lJob));
        Assert.assertEquals(rtJob, flowDirectory.getDelegatingJobParent(retryJob));
        
        logger.info("End testFlowDirectory");              
    }
}
