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
package azkaban.jobs.builtin;

import azkaban.app.JobDescriptor;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;

/**
 *
 */
public class NoopJob implements Job
{
    public NoopJob(JobDescriptor descriptor)
    {
        
    }

    @Override
    public String getId()
    {
        return "Azkaban!! -- " + getClass().getName();
    }

    @Override
    public void run() throws Exception
    {
    }

    @Override
    public void cancel() throws Exception
    {
    }

    @Override
    public double getProgress() throws Exception
    {
        return 0;
    }

    @Override
    public Props getJobGeneratedProperties()
    {
        return new Props();
    }

    @Override
    public boolean isCanceled() {
        return false;
    }
}
