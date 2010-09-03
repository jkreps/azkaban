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

package azkaban.common.jobs;

import azkaban.common.utils.Props;

public abstract class DelegatingJob implements Job {

    private final Job _innerJob;

    public DelegatingJob(Job innerJob) {
        _innerJob = innerJob;
    }

    public Job getInnerJob() {
        return _innerJob;
    }

    public void cancel() throws Exception {
        _innerJob.cancel();
    }

    public String getId() {
        return _innerJob.getId();
    }

    public double getProgress() throws Exception {
        return _innerJob.getProgress();
    }

    @Override
    public Props getJobGeneratedProperties()
    {
        return _innerJob.getJobGeneratedProperties();
    }

    public abstract void run() throws Exception;

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "_innerJob=" + _innerJob +
               '}';
    }
}
