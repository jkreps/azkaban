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



public class PythonJob extends LongArgJob {
    
    private static final String PYTHON_BINARY_KEY = "python";
    private static final String SCRIPT_KEY = "script";


    public PythonJob(JobDescriptor desc) {
        super(new String[]{desc.getProps().getString(PYTHON_BINARY_KEY, "python"), 
                           desc.getProps().getString(SCRIPT_KEY)}, 
              desc, 
              ImmutableSet.of(PYTHON_BINARY_KEY, SCRIPT_KEY, JobDescriptor.JOB_TYPE));
    }



    
}
