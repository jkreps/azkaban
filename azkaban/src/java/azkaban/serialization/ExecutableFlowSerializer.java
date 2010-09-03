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

import azkaban.flow.*;
import com.google.common.base.Function;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ExecutableFlowSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final AtomicBoolean serializersBeenSet = new AtomicBoolean(false);

    private volatile Map<Class, Function<ExecutableFlow, Map<String, Object>>> serializers;

    public void setSerializers(Map<Class, Function<ExecutableFlow, Map<String, Object>>> serializers)
    {
        if (serializers != null && serializersBeenSet.compareAndSet(false, true)) {
            this.serializers = serializers;
        }
        else {
            throw new IllegalStateException(String.format(
                    "Serializers cannot null nor can they be set more than once, value passed in was [%s]",
                    serializers
            ));
        }
    }

    @Override
    public Map<String,  Object> apply(ExecutableFlow executableFlow)
    {
        if (! serializersBeenSet.get()) {
            throw new IllegalStateException("Serializers have not been set yet.  Call setSerializers before calling apply.");
        }


        Function<ExecutableFlow, Map<String, Object>> subSerializer = serializers.get(executableFlow.getClass());

        if (subSerializer == null) {
            if (executableFlow instanceof WrappingExecutableFlow) {
                return apply(((WrappingExecutableFlow) executableFlow).getDelegateFlow());
            }

            throw new RuntimeException(String.format("No known serializer for class[%s].", executableFlow.getClass()));
        }

        return subSerializer.apply(executableFlow);
    }
}
