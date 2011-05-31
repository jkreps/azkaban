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
package azkaban.common.jobs;

import junit.framework.Assert;

import org.junit.Test;

import azkaban.common.utils.Props;

/**
 * @author Ibrahim Ulukaya
 *
 */
public class DoNothingJobTest {

    @Test
    public void testDoNothingJob() {
        // Start a fictitious job.
        String wfId = "23";
        String job1Name = "Job1";
        Props job1Props = new Props();
        job1Props.put("azkaban.flow.id", wfId);
        DoNothingJob job1 = new DoNothingJob(job1Name, job1Props);

        Assert.assertNotNull(job1.getJobGeneratedProperties());
    }
}
