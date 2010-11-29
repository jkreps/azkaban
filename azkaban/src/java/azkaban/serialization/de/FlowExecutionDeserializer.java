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
package azkaban.serialization.de;

import azkaban.common.utils.Props;
import azkaban.flow.FlowExecutionHolder;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;

import java.util.Map;

/**
 * Deserializes a "flow execution" object.  I.e., one of the JSONObjects
 * in the json files in the execution history directory.
 *
 */
public class FlowExecutionDeserializer implements Function<Map<String, Object>, FlowExecutionHolder>
{
    private final ExecutableFlowDeserializer flowDeserializer;

    public FlowExecutionDeserializer(
            ExecutableFlowDeserializer flowDeserializer
    )
    {
        this.flowDeserializer = flowDeserializer;
    }

    @Override
    public FlowExecutionHolder apply(Map<String, Object> descriptor) {
        final FlowExecutionHolder retVal;

        // Maintain backwards compatibility, check if it's really a holder
        final Object type = descriptor.get("type");
        if (type != null && "FlowExecutionHolder".equals(type.toString())) {
            retVal = new FlowExecutionHolder(
                    flowDeserializer.apply(Verifier.getVerifiedObject(descriptor, "flow", Map.class)),
                    new Props(new Props(), Verifier.getVerifiedObject(descriptor, "parentProps", Map.class))
            );
        }
        else {
            retVal = new FlowExecutionHolder(
                    flowDeserializer.apply(descriptor),
                    new Props()
            );
        }

        return retVal;
    }
}
