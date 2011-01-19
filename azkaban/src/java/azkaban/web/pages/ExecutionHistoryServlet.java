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

package azkaban.web.pages;

import azkaban.common.web.Page;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.flow.Flows;
import azkaban.jobs.JobExecutionException;
import azkaban.web.AbstractAzkabanServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExecutionHistoryServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        final FlowManager allFlows = this.getApplication().getAllFlows();

        /* set runtime properties from request and response*/
        super.setRuntimeProperties (req, resp);
        
        if (hasParam(req, "action")) {
            if ("restart".equals(getParam(req, "action")) && hasParam(req, "id")) {
                try {
                    long id = Long.parseLong(getParam(req, "id"));

                    final FlowExecutionHolder holder = allFlows.loadExecutableFlow(id);

                    if (holder == null) {
                        addMessage(req, String.format("Unknown flow with id[%s]", id));
                    }
                    else {
                        Flows.resetFailedFlows(holder.getFlow());
                        this.getApplication().getJobExecutorManager().execute(holder);

                        addMessage(req, String.format("Flow[%s] restarted.", id));
                    }
                }
                catch (NumberFormatException e) {
                    addMessage(req, String.format("Apparently [%s] is not a valid long.", getParam(req, "id")));
                }
                catch (JobExecutionException e) {
                	addMessage(req, "Error restarting " + getParam(req, "id") + ". " + e.getMessage());
                }
            }
        }

        long currMaxId = allFlows.getCurrMaxId();

        String beginParam = req.getParameter("begin");
        int begin = beginParam == null? 0 : Integer.parseInt(beginParam);
        
        String sizeParam = req.getParameter("size");
        int size = sizeParam == null? 20 : Integer.parseInt(sizeParam);

        List<ExecutableFlow> execs = new ArrayList<ExecutableFlow>(size);
        for (int i = begin; i < begin + size; i++) {
            final FlowExecutionHolder holder = allFlows.loadExecutableFlow(currMaxId - i);
            ExecutableFlow flow = null;
            if (holder != null)
                flow = holder.getFlow();

            if (flow != null)
                execs.add(flow);
        }

        Page page = newPage(req, resp, "azkaban/web/pages/execution_history.vm");
        page.add("executions", execs);
        page.add("begin", begin);
        page.add("size", size);
        page.render();
    }

}
