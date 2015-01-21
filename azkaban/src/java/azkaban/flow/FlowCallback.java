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

package azkaban.flow;

import azkaban.jobs.Status;

/**
 * A callback to be used when running jobs through a Flow.
 *
 * There is no guarantee about what thread will call a callback.
 */
public interface FlowCallback {

    /**
     * Method called whenever some sub-set of the flow is complete.
     */
    public void progressMade();

    /**
     * Method called when the entire flow has completed and does not have anything else running.
     *
     * @param status the status that the flow ended with.
     */
    public void completed(final Status status);
}
