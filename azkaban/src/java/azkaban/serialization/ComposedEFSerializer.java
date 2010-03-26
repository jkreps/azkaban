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

import azkaban.flow.ComposedExecutableFlow;
import azkaban.flow.ExecutableFlow;
import azkaban.util.FoldLeft;
import com.google.common.base.Function;
import com.google.common.collect.*;

import java.util.*;

/**
 *
 */
public class ComposedEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> globalSerializer;

    public ComposedEFSerializer(Function<ExecutableFlow, Map<String, Object>> globalSerializer)
    {
        this.globalSerializer = globalSerializer;
    }

    @Override
    public Map<String,  Object> apply(ExecutableFlow executableFlow)
    {
        ComposedExecutableFlow flow = (ComposedExecutableFlow) executableFlow;

        final Map<String, Object> dependeeMap = globalSerializer.apply(flow.getDependee());
        final Map<String, Object> dependerMap = globalSerializer.apply(flow.getDepender());


        Map<String, Object> retVal = new HashMap<String, Object>();

        retVal.put(
                "jobs",
                FoldLeft.fold(
                        Iterables.transform(
                                Arrays.asList(dependeeMap, dependerMap),
                                new Function<Map<String, Object>, Map<String, Object>>()
                                {
                                    @Override
                                    public Map<String, Object> apply(Map<String, Object> descriptor)
                                    {
                                        return Verifier.getVerifiedObject(descriptor, "jobs", Map.class);
                                    }
                                }
                        ),
                        new HashMap<String, Object>(),
                        new FoldLeft<Map<String, Object>, HashMap<String, Object>>()
                        {
                            @Override
                            public HashMap<String, Object> fold(HashMap<String, Object> oldValue, Map<String, Object> newValue)
                            {
                                for (Map.Entry<String, Object> entry : newValue.entrySet()) {
                                    final String key = entry.getKey();

                                    if (! oldValue.containsKey(key)) {
                                        oldValue.put(key, entry.getValue());
                                    }
                                }

                                return oldValue;
                            }
                        }
                )
        );

        retVal.put("root", Verifier.getVerifiedObject(dependerMap, "root", List.class));

        Map<String, Object> dependenciesMap = new HashMap<String, Object>();
        for (Object o : Verifier.getVerifiedObject(dependeeMap, "dependencies", Map.class).entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            dependenciesMap.put(entry.getKey().toString(), entry.getValue());
        }

        for (Object o : Verifier.getVerifiedObject(dependerMap, "dependencies", Map.class).entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String key = entry.getKey().toString();

            if (dependenciesMap.containsKey(key)) {
                dependenciesMap.put(
                        key,
                        Sets.newTreeSet(Iterables.concat((Set) entry.getValue(), (Set) dependenciesMap.get(key)))
                );
            }
            else {
                dependenciesMap.put(key, entry.getValue());
            }
        }

        for (Object o : Verifier.getVerifiedObject(dependerMap, "root", List.class)) {
            Set<String> set = new TreeSet<String>();
            String key = o.toString();

            for (Object o1 : Verifier.getVerifiedObject(dependeeMap, "root", List.class)) {
                set.add(o1.toString());
            }

            if (dependenciesMap.containsKey(key)) {
                dependenciesMap.put(
                        key,
                        Sets.newTreeSet(Iterables.concat(set, (Set) dependenciesMap.get(key)))
                );
            }
            else {
                dependenciesMap.put(key, set);
            }
        }

        retVal.put("dependencies", dependenciesMap);
        retVal.put("id", flow.getId());

        return retVal;
    }
}
