/*
 * Copyright (C) 2011 Adconion, Inc
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
package azkaban.jobs;

import java.util.Map;

import org.easymock.classextension.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.app.JobDescriptor;
import azkaban.common.utils.Props;
import azkaban.jobs.AbstractProcessJob;
import azkaban.jobs.builtin.ProcessJob;

/**
 * @author Ibrahim Ulukaya
 */
public class AbstractProcessJobTest {
    private AbstractProcessJob job = null;
    private JobDescriptor descriptor = null;
    private Props props = null;
    @Before
    public void setUp() {

      /*  initialize job */
      descriptor = EasyMock.createMock(JobDescriptor.class);

      props = new Props();
      props.put(AbstractProcessJob.WORKING_DIR, ".");

      EasyMock.expect(descriptor.getId()).andReturn("process").times(1);
      EasyMock.expect(descriptor.getProps()).andReturn(props).times(1);
      EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);

      EasyMock.replay(descriptor);

      job = new ProcessJob(descriptor);

      EasyMock.verify(descriptor);
    }

    @Test
    public void testEnvVariables()
    {
        props.put("type", "command");
        props.put("command", "sh ./foobar.sh");
        props.put("ENV.FOO", "bar");
        props.put("env.FOO_BAR", "baz");
        Map <String, String> envMap = job.getEnvironmentVariables();
        Assert.assertEquals(2, envMap.size());
        Assert.assertEquals("baz", envMap.get("FOO_BAR"));
        Assert.assertEquals("bar", envMap.get("FOO"));
    }
}
