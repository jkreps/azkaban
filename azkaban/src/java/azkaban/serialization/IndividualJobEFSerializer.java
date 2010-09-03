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
import azkaban.flow.IndividualJobExecutableFlow;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IndividualJobEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    @Override
    public Map<String, Object> apply(ExecutableFlow executableFlow)
    {
        IndividualJobExecutableFlow flow = (IndividualJobExecutableFlow) executableFlow;
        Map<String, Object> retVal = new HashMap<String, Object>();

        // "Jobs" should really be first-class, serializable things so that they can be stored
        // along with their execution-specific properties, and stuff.  But, that's a more involved
        // change than I can make right now.
        // TODO MED: Fix the above.
        final String jobName = flow.getName();

        ImmutableMap.Builder<String, Object> jobInfoMapBuilder = ImmutableMap.builder();
        
        jobInfoMapBuilder.put("type", "jobManagerLoaded");
        jobInfoMapBuilder.put("name", jobName);
        jobInfoMapBuilder.put("status", flow.getStatus().toString());
        jobInfoMapBuilder.put("id", flow.getId());
        if (flow.getParentProps() != null) {
            jobInfoMapBuilder.put("overrideProps", flow.getParentProps().getMapByPrefix(""));
        }

        if (flow.getReturnProps() != null) {
            jobInfoMapBuilder.put("returnProps", flow.getReturnProps().getMapByPrefix(""));
        }

        if (flow.getStartTime() != null) {
            jobInfoMapBuilder.put("startTime", flow.getStartTime().toString());
        }

        if (flow.getEndTime() != null) {
            jobInfoMapBuilder.put("endTime", flow.getEndTime().toString());
        }

        retVal.put("jobs", ImmutableMap.<String, Object>of(jobName, jobInfoMapBuilder.build()));
        retVal.put("root", Arrays.asList(jobName));
        retVal.put("dependencies", Collections.<String, Object>emptyMap());
        retVal.put("id", flow.getId());

        return retVal;
    }
}
