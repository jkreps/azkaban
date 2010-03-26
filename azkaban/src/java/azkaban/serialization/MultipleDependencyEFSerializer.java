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

package azkaban.serialization;

import azkaban.flow.ExecutableFlow;
import azkaban.flow.MultipleDependencyExecutableFlow;
import com.google.common.base.Function;

import java.util.Map;

/**
 *
 */
public class MultipleDependencyEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> globalSerializer;

    public MultipleDependencyEFSerializer(Function<ExecutableFlow, Map<String, Object>> globalSerializer)
    {
        this.globalSerializer = globalSerializer;
    }

    @Override
    public Map<String, Object> apply(ExecutableFlow executableFlow)
    {
        MultipleDependencyExecutableFlow flow = (MultipleDependencyExecutableFlow) executableFlow;

        return globalSerializer.apply(flow.getActualFlow());
    }
}
