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
 * A placeholder job that does nothing. Used for defining barriers for
 * convenient dependency management
 * 
 * E.g. this job can be used solely as a way of grouping dependencies
 * 
 * @author jkreps
 * 
 */
public class DoNothingJob extends AbstractJob {

    public DoNothingJob(String name, Props props) {
        super(name);
    }

    public void run() {
    // does nothing, just a placeholder for dependencies
    }

    @Override
    public Props getJobGeneratedProperties() {
        // TODO Auto-generated method stub
        return null;
    }

}
