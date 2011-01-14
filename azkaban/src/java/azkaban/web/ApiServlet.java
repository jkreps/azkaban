package azkaban.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.app.AzkabanApplication;
import azkaban.jobs.JobExecutionException;
import azkaban.util.json.JSONUtils;


public class ApiServlet extends AbstractAzkabanServlet {
    /**
	 * 
	 */
	private static final long serialVersionUID = 246975794895355127L;
	private static final Logger logger = Logger.getLogger(ApiServlet.class.getName());
    
	private static JSONUtils jsonUtils = new JSONUtils();
	
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
    	handleCall(req, resp);
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
    	handleCall(req, resp);
    }
    
    private void handleCall(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	if (!hasParam(req, "action")) {
    		PrintWriter writer = resp.getWriter();
    		writer.print("ApiServlet");
    		return;
    	}
    	
    	String action = getParam(req, "action");
    	if (action.equals("run_job")) {
    		handleRunJob(req, resp);
    	}
    }
    
    private void handleRunJob(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String id = getParam(req, "id");
        resp.setContentType("application/json");
		boolean ignoreDeps = !Boolean.parseBoolean(getParam(req, "include_deps"));
		PrintWriter writer = resp.getWriter();
		
        AzkabanApplication app = getApplication();
		HashMap<String, Object> results = new HashMap<String, Object>();
		try {
        	app.getJobExecutorManager().execute(id, ignoreDeps);
        	results.put("success", "Running " + id + (ignoreDeps ? " without dependencies." : " with dependencies."));
        }
        catch (JobExecutionException e) {
        	results.put("error", e.getMessage());
        }
        
    	writer.print(jsonUtils.toJSONString(results));
    	writer.flush();
    	
    	writer.close();
    }
}