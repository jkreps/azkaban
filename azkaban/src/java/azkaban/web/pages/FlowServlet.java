package azkaban.web.pages;

import azkaban.app.AzkabanApp;
import azkaban.flow.*;
import azkaban.serialization.*;
import azkaban.serialization.de.ExecutableFlowDeserializer;
import azkaban.serialization.de.JobFlowDeserializer;
import azkaban.flow.JobManagerFlowDeserializer;
import azkaban.web.AbstractAzkabanServlet;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class FlowServlet extends AbstractAzkabanServlet
{
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        AzkabanApp app = this.getApplication();


        ExecutableFlowDeserializer deserializer = new ExecutableFlowDeserializer(
                new JobFlowDeserializer(
                        ImmutableMap.<String, Function<Map<String, Object>, ExecutableFlow>>of(
                                "jobManagerLoaded", new JobManagerFlowDeserializer(app.getJobManager())
                        )
                )
        );

        ExecutableFlowSerializer serializer = new DefaultExecutableFlowSerializer();

        JSONObject obj = new JSONObject(serializer.apply(app.getAllFlows().createNewExecutableFlow("pymk")));
        byte[] returnMe;
        try {
            returnMe = obj.toString(2).getBytes();
        }
        catch (JSONException e) {
            throw new ServletException(e);
        }

        resp.setStatus(200);
        resp.setContentType("application/json");
        resp.setContentLength(returnMe.length);
        resp.getOutputStream().write(returnMe);
        resp.flushBuffer();
    }
}
