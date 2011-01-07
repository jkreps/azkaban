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

import azkaban.common.utils.Props;
import azkaban.common.web.Page;
import azkaban.flow.ComposedExecutableFlow;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.flow.MultipleDependencyExecutableFlow;
import azkaban.flow.WrappingExecutableFlow;
import azkaban.jobs.Status;
import azkaban.util.json.JSONUtils;
import azkaban.web.AbstractAzkabanServlet;
import azkaban.workflow.Flow;
import azkaban.workflow.flow.DagLayout;
import azkaban.workflow.flow.Dependency;
import azkaban.workflow.flow.FlowNode;
import azkaban.workflow.flow.SugiyamaLayout;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class FlowExecutionServlet extends AbstractAzkabanServlet {
	private static final long serialVersionUID = 7234050895543142356L;

	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        resp.setContentType("application/xhtml+xml");
        Page page = newPage(req, resp, "azkaban/web/pages/flow_instance.vm");
        final FlowManager allFlows = this.getApplication().getAllFlows();
     
        if (hasParam(req, "job_id")) {
        	String jobID = getParam(req, "job_id");
        	ExecutableFlow flow = allFlows.createNewExecutableFlow(jobID);
        	
        	page.add("id", "0");
        	page.add("name", jobID);
        	
        	if (flow == null) {
        		addError(req, "Job " + jobID + " not found.");
        		page.render();
        		return;
        	}
        	
        	// This will be used the other
        	Flow displayFlow = new Flow(flow.getName(), (Props)null);
        	fillFlow(displayFlow, flow);
        	displayFlow.validateFlow();
        	
        	String flowJSON = createJsonFlow(displayFlow);
        	page.add("jsonflow", flowJSON);
        	page.add("action", "run");
        	page.add("joblist", createJsonJobList(displayFlow));
        }
        else if (hasParam(req, "id")) {
        	long id = Long.parseLong(getParam(req, "id"));
           	FlowExecutionHolder holder = allFlows.loadExecutableFlow(id);
        	ExecutableFlow executableFlow = holder.getFlow();

        	// This will be used the other
        	Flow displayFlow = new Flow(executableFlow.getName(), (Props)null);
        	fillFlow(displayFlow, executableFlow);
        	displayFlow.validateFlow();
        	
        	String flowJSON = createJsonFlow(displayFlow);
        	page.add("jsonflow", flowJSON);
        	page.add("id", id);
        	if (executableFlow.getStartTime() != null) {
        		page.add("startTime", executableFlow.getStartTime());
        		if (executableFlow.getEndTime() != null) {
            		page.add("endTime", executableFlow.getEndTime());
        			page.add("period", new Duration(executableFlow.getStartTime(), executableFlow.getEndTime()).toPeriod());
        		}
        		else {
        			page.add("period", new Duration(executableFlow.getStartTime(), new DateTime()).toPeriod());
        		}
        	}

        	page.add("showTimes", true);
        	page.add("name", executableFlow.getName());
        	page.add("action", "restart");
        	page.add("joblist", createJsonJobList(displayFlow));
        }

        page.render();
    }
    
    private void fillFlow(Flow displayFlow, ExecutableFlow executableFlow) {
    	List<String> dependencies = new ArrayList<String>();
    	for( ExecutableFlow depFlow : executableFlow.getChildren()) {
    		dependencies.add(depFlow.getName());
    		fillFlow(displayFlow, depFlow);
    	}
    	
    	displayFlow.addDependencies(executableFlow.getName(), dependencies);
    	displayFlow.setStatus(executableFlow.getName(), getStringStatus(executableFlow.getStatus()));
    }
    
    private String getStringStatus(Status status) {
    	switch(status) {
    	case COMPLETED:
    		return "completed";
    	case FAILED:
    		return "failed";
    	case SUCCEEDED:
    		return "succeeded";
    	case RUNNING:
    		return "running";
    	case READY:
    		return "ready";
    	case IGNORED:
    	    return "disabled";
    	}
    
    	
    	return "normal";
    }
    
	@SuppressWarnings("unchecked")
	private String createJsonJobList(Flow flow) {
		JSONArray jsonArray = new JSONArray();
    	for (FlowNode node : flow.getFlowNodes()) {
    		jsonArray.add(node.getAlias());
    	}
		
    	return jsonArray.toJSONString();
    }
    
	@SuppressWarnings("unchecked")
	private String createJsonFlow(Flow flow) {
		JSONObject jsonFlow = new JSONObject();
		jsonFlow.put("flow_id", flow.getId());

		if (!flow.isLayedOut()) {
			DagLayout layout = new SugiyamaLayout(flow);
			layout.setLayout();
		}

		JSONArray jsonNodes = new JSONArray();

		for (FlowNode node : flow.getFlowNodes()) {
			JSONObject jsonNode = new JSONObject();
			jsonNode.put("name", node.getAlias());
			jsonNode.put("x", node.getX());
			jsonNode.put("y", node.getY());
			jsonNode.put("status", node.getStatus());
			jsonNodes.add(jsonNode);
		}

		JSONArray jsonDependency = new JSONArray();

		for (Dependency dep : flow.getDependencies()) {
			JSONObject jsonDep = new JSONObject();
			jsonDep.put("dependency", dep.getDependency().getAlias());
			jsonDep.put("dependent", dep.getDependent().getAlias());
			jsonDependency.add(jsonDep);
		}

		jsonFlow.put("nodes", jsonNodes);
		jsonFlow.put("timestamp", flow.getLastModifiedTime());
		jsonFlow.put("layouttimestamp", flow.getLastLayoutModifiedTime());
		jsonFlow.put("dependencies", jsonDependency);

		return jsonFlow.toJSONString();
	}
	
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json");

        final FlowManager allFlows = this.getApplication().getAllFlows();
        String action = getParam(req, "action");
        
        if (action.equals("restart")) {
        	String value = req.getParameter("disabled");
        	String[] disabledValues = value.split(",");
        	HashSet<String> disabledJobs = new HashSet<String>();
        	for (String disabled : disabledValues) {
        		if (!disabled.isEmpty()) {
        			disabledJobs.add(disabled);
        		}
        	}
        	
        	long id = Long.parseLong(getParam(req, "id"));
           	FlowExecutionHolder holder = allFlows.loadExecutableFlow(id);
        	//Flows.resetFailedFlows(holder.getFlow());
        	
        	// Disable all proper values
        	ExecutableFlow executableFlow = holder.getFlow();
        	traverseFlow(disabledJobs, executableFlow);
        	
    		PrintWriter writer = resp.getWriter();
    		JSONUtils jsonUtils = new JSONUtils();
    		HashMap<String,Object> results = new HashMap<String,Object>();
    		
        	try {
        		this.getApplication().getJobExecutorManager().execute(holder);
        		results.put("id", holder.getFlow().getId());
        		results.put("success", true);
        		results.put("message", String.format("Executing Flow[%s].", id));
        	} catch(Exception e) {
        		results.put("id", holder.getFlow().getId());
        		results.put("error", true);
        		results.put("message", String.format("Error running Flow[%s]. " + e.getMessage(), id));
        	}
        	
        	writer.print(jsonUtils.toJSONString(results));
        	writer.flush();
        }
        else if (action.equals("run")) {
        	String name = getParam(req, "name");
        	String value = req.getParameter("disabled");
        	String[] disabledValues = value.split(",");
        	HashSet<String> disabledJobs = new HashSet<String>();
        	for (String disabled : disabledValues) {
        		if (!disabled.isEmpty()) {
        			disabledJobs.add(disabled);
        		}
        	}
        	
           	ExecutableFlow flow = allFlows.createNewExecutableFlow(name);
           	if (flow == null) {
        		addError(req, "Job " + name + " not found.");
           	}
           	traverseFlow(disabledJobs, flow);
    		PrintWriter writer = resp.getWriter();
    		JSONUtils jsonUtils = new JSONUtils();
    		HashMap<String,Object> results = new HashMap<String,Object>();
    		
        	try {
        		this.getApplication().getJobExecutorManager().execute(flow);
        		results.put("success", true);
        		results.put("message", String.format("Executing Flow[%s].", name));
        		results.put("id", flow.getId());
            	
        	} catch(Exception e) {
        		results.put("error", true);
        		results.put("message", String.format("Error running Flow[%s]. " + e.getMessage(), name));
        	}
        	
        	writer.print(jsonUtils.toJSONString(results));
        	writer.flush();
        }
        
       
    }
    
    private void traverseFlow(HashSet<String> disabledJobs, ExecutableFlow flow) {
    	String name = flow.getName();
		System.out.println("at " + name);
    	flow.reset();
    	if (flow instanceof IndividualJobExecutableFlow && disabledJobs.contains(name)) {
    		IndividualJobExecutableFlow individualJob = (IndividualJobExecutableFlow)flow;
    		individualJob.setStatus(Status.IGNORED);
    		System.out.println("ignore " + name);
    	}
    	else {
    		if (flow instanceof ComposedExecutableFlow) {
        		ExecutableFlow innerFlow = ((ComposedExecutableFlow) flow).getDepender();
        		traverseFlow(disabledJobs, innerFlow);
    		}
    		else if (flow instanceof MultipleDependencyExecutableFlow) {
        		traverseFlow(disabledJobs, ((MultipleDependencyExecutableFlow) flow).getActualFlow());
    		}
    		else if (flow instanceof WrappingExecutableFlow) {
        		traverseFlow(disabledJobs, ((WrappingExecutableFlow) flow).getDelegateFlow());
    		}
    		
    		for(ExecutableFlow childFlow : flow.getChildren()) {
    			traverseFlow(disabledJobs, childFlow);
    		}
    	}
    	
    }
}
