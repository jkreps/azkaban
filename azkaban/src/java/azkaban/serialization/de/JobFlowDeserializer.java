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

import azkaban.flow.ExecutableFlow;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;

import java.util.Map;

/**
 *
 */
public class JobFlowDeserializer implements Function<Map<String, Object>, ExecutableFlow>
{
    private final Map<String, Function<Map<String, Object>, ExecutableFlow>> baseDeserializerMap;

    public JobFlowDeserializer(Map<String, Function<Map<String, Object>, ExecutableFlow>> baseDeserializerMap)
    {

        this.baseDeserializerMap = baseDeserializerMap;
    }

    @Override
    public ExecutableFlow apply(Map<String, Object> descriptor)
    {
        String type = Verifier.getString(descriptor, "type");

        Function<Map<String, Object>, ExecutableFlow> deserializer = baseDeserializerMap.get(type);
        if (deserializer == null) {
            throw new RuntimeException(
                    String.format(
                            "No deserializer for type[%s].  Known types are [%s]",
                            type,
                            baseDeserializerMap.keySet())
            );                    
        }

        return deserializer.apply(descriptor);
    }
}
