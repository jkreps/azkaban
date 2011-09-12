/*
 * Copyright 2011 Adconion, Inc.
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
package azkaban.app;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.common.jobs.Job;
import azkaban.jobs.builtin.ProcessJob;

public class AzkabanApplicationTest {

    private File jobDir;
    private File logDir;
    private File tmpDir;

    @Before
    public void setUp() throws IOException {
        jobDir = mktempdir("azkaban_jobs");
        logDir = mktempdir("azkaban_logs");
        tmpDir = mktempdir("azkaban_tmp");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(jobDir);
        FileUtils.deleteDirectory(logDir);
        FileUtils.deleteDirectory(tmpDir);
    }

    private File mktempdir(String name) throws IOException {
        File dir = File.createTempFile(name, ".d");
        FileUtils.forceDelete(dir);
        FileUtils.forceMkdir(dir);
        return dir;
    }

    @Test
    public void testAddJobAndReload() throws Exception {
        String testJob = "testjob";
        AzkabanApplication app = new AzkabanApplication(Arrays.asList(jobDir),
                logDir, tmpDir, false);
        Assert.assertEquals(0, app.getJobManager().loadJobDescriptors().size());
        Assert.assertNull(app.getJobManager().getJobDescriptor(testJob));
        
        File newJobDir = new File(jobDir, "test");
        FileUtils.forceMkdir(newJobDir);
        File newJob = new File(newJobDir, testJob + ".job");
        FileUtils.writeLines(newJob, Arrays.asList("type=command", "command=ls"));
        
        app.reloadJobsFromDisk();
        
        Assert.assertEquals(1, app.getJobManager().loadJobDescriptors().size());
        Job loadedJob = app.getJobManager().loadJob(testJob, true);
        
        Assert.assertEquals(testJob, loadedJob.getId());
        Assert.assertTrue(loadedJob instanceof LoggingJob);
        Assert.assertTrue(((LoggingJob)loadedJob).getInnerJob() instanceof ProcessJob);
    }
}
