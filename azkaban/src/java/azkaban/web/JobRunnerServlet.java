package azkaban.web;

import azkaban.app.PropsUtils;
import azkaban.common.utils.Props;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.util.JSONToJava;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 */
public class JobRunnerServlet extends AbstractAzkabanServlet
{
    private static final JSONToJava jsonConverter = new JSONToJava();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String jobName = req.getPathInfo();

        if (jobName == null || jobName.length() < 2) {
            resp.sendError(400, "Path must end with job name to run.");
            return;
        }

        jobName = jobName.substring(1); // Remove the "/" prefix

        final FlowManager flowManager = getApplication().getAllFlows();
        ExecutableFlow flowToRun = flowManager.createNewExecutableFlow(jobName);

        if (flowToRun == null) {
            resp.sendError(404, String.format("Unknown job[%s]", jobName));
            return;
        }

        Props parentProps = PropsUtils.produceParentProperties(flowToRun);
        final String contentType = req.getContentType();
        if (contentType != null) {
            if (contentType.startsWith("application/json")) {
                JSONObject obj = null;
                try {
                    obj = new JSONObject(IOUtils.toString(req.getInputStream()));
                } catch (JSONException e) {
                    resp.sendError(400, String.format("Bad JSON object."));
                }

                Map propertyMap = jsonConverter.apply(obj);
                parentProps = new Props(parentProps, propertyMap);
            }
            else {
                resp.sendError(400, String.format("Cannot handle content type[%s]", contentType));
                return;
            }
        }

        getApplication().getJobExecutorManager().execute(
                new FlowExecutionHolder(
                        flowToRun,
                        parentProps
                )
        );

        resp.setStatus(200);
    }
}
