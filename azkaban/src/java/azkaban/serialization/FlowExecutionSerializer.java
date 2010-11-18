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
import azkaban.flow.FlowExecutionHolder;
import com.google.common.base.Function;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 */
public class FlowExecutionSerializer implements Function<FlowExecutionHolder, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> executableFlowSerializer;


    public FlowExecutionSerializer(
            Function<ExecutableFlow, Map<String, Object>> executableFlowSerializer
    )
    {
        this.executableFlowSerializer = executableFlowSerializer;
    }

    @Override
    public Map<String, Object> apply(FlowExecutionHolder flowExecutionHolder) {
        Map<String, Object> retVal = new LinkedHashMap<String, Object>();

        final Map<String, String> propsToPut;
        if (flowExecutionHolder.getParentProps() != null) {
            propsToPut = flowExecutionHolder.getParentProps().getMapByPrefix("");
        } else {
            propsToPut = Collections.emptyMap();
        }

        retVal.put("type", "FlowExecutionHolder");
        retVal.put("flow", executableFlowSerializer.apply(flowExecutionHolder.getFlow()));
        retVal.put("parentProps", propsToPut);

        return retVal;
    }
}
