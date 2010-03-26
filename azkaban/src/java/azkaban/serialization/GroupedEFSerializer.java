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
import azkaban.flow.GroupedExecutableFlow;
import azkaban.util.FoldLeft;
import com.google.common.base.Function;
import com.google.common.collect.*;

import java.util.*;

/**
 *
 */
public class GroupedEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> globalSerializer;

    public GroupedEFSerializer(Function<ExecutableFlow, Map<String, Object>> globalSerializer)
    {
        this.globalSerializer = globalSerializer;
    }

    @Override
    public Map<String, Object> apply(ExecutableFlow executableFlow)
    {
        GroupedExecutableFlow flow = (GroupedExecutableFlow) executableFlow;

        List<ExecutableFlow> children = flow.getChildren();
        List<Map<String, Object>> childMaps = new ArrayList<Map<String, Object>>(children.size());

        for (ExecutableFlow child : children) {
            childMaps.add(globalSerializer.apply(child));
        }

        Map<String, Object> retVal = new HashMap<String, Object>();

        Map<String, Object> jobsMap = new HashMap<String, Object>();
        for (Map<String, Object> childMap : childMaps) {
            Map<String, Object> childJobsMap = Verifier.getVerifiedObject(childMap, "jobs", Map.class);

            for (Map.Entry<String, Object> entry : childJobsMap.entrySet()) {
                final String key = entry.getKey();

                if (! jobsMap.containsKey(key)) {
                    jobsMap.put(key, entry.getValue());
                }
            }
        }

        retVal.put("jobs", jobsMap);


        List<Object> rootList = new ArrayList<Object>(childMaps.size());
        for (Map<String, Object> childMap : childMaps) {
            for (Object root : Verifier.getVerifiedObject(childMap, "root", List.class)) {
                rootList.add(root);
            }
        }

        retVal.put("root", rootList);


        Map<String, Object> dependenciesMap = new HashMap<String, Object>();
        for (Map<String, Object> childMap : childMaps) {
            for (Object o : Verifier.getVerifiedObject(childMap, "dependencies", Map.class).entrySet()) {
                Map.Entry entry = (Map.Entry<String, Object>) o;

                String key = entry.getKey().toString();

                Object value = entry.getValue();
                if (value == null || !(value instanceof Set)) {
                    throw new RuntimeException(String.format(
                            "Key[%s] pointed to a %s instead of a %s.  Map[%s]",
                            key,
                            value.getClass(),
                            Set.class,
                            childMaps
                    ));
                }
                Set newDependencies = (Set) value;

                if (dependenciesMap.containsKey(key)) {
                    Set existingDependencies = Verifier.getVerifiedObject(dependenciesMap, key, Set.class);

                    dependenciesMap.put(key, Sets.newTreeSet(Iterables.concat(existingDependencies, newDependencies)));
                }
                else {
                    dependenciesMap.put(key, newDependencies);
                }
            }
        }

        retVal.put("dependencies", dependenciesMap);
        retVal.put("id", flow.getId());

        return retVal;
    }
}
