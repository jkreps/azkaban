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

package azkaban.app.jmx;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.common.utils.Props;

/**
 * @author ibrahimulukaya
 *
 */
public class MockJobManager extends JobManager{

    public MockJobManager()
    {
        super(null,null,null,null,null);
    }

    @Override
    public JobDescriptor getJobDescriptor(String name)
    {
        if (name.startsWith("test")) {
            return new JobDescriptor(name, null, null, new Props(), null);
        }
        else {
            return null;
        }
    }

}
