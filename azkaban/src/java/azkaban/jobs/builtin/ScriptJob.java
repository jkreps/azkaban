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

import com.google.common.collect.ImmutableSet;

import azkaban.app.JobDescriptor;

/**
 * A script job issues a command of the form
 *    [EXECUTABLE] [SCRIPT] --key1 val1 ... --key2 val2
 *   executable -- the interpretor command to execute
 *   script -- the script to pass in (requried)
 * 
 * @author jkreps
 *
 */
public class ScriptJob extends LongArgJob {

    private static final String DEFAULT_EXECUTABLE_KEY = "executable";
    private static final String SCRIPT_KEY = "script";
    
    public ScriptJob(JobDescriptor desc) {
        super(new String[] {desc.getProps().getString(DEFAULT_EXECUTABLE_KEY), desc.getProps().getString(SCRIPT_KEY)}, 
              desc, 
              ImmutableSet.of(DEFAULT_EXECUTABLE_KEY, SCRIPT_KEY, JobDescriptor.JOB_TYPE));
    }
 

}
