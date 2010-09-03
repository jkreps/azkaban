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

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

/**
 * A Job that just sleeps for the given amount of time
 * 
 * @author jkreps
 * 
 */
public class SleepyJob extends AbstractJob {

    private final long _sleepTimeMs;
    private Props _props =  new Props();

    public SleepyJob(String name, Props p) {
        super(name);
        this._sleepTimeMs = p.getLong("sleep.time.ms", Long.MAX_VALUE);
    }

    public void run() {
        info("Sleeping for " + _sleepTimeMs + " ms.");
        try {
            Thread.sleep(_sleepTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Props getJobGeneratedProperties() {
        return _props;
    }

}
