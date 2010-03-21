package azkaban.web.pages;

import azkaban.common.web.Page;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowCallback;
import azkaban.flow.manager.FlowManager;
import azkaban.flow.Flows;
import azkaban.flow.Status;
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

        if (hasParam(req, "action")) {
            if ("restart".equals(getParam(req, "action")) && hasParam(req, "id")) {
                try {
                    long id = Long.parseLong(getParam(req, "id"));

                    final ExecutableFlow flow = allFlows.loadExecutableFlow(id);

                    if (flow == null) {
                        addMessage(req, String.format("Unknown flow with id[%s]", id));
                    }
                    else {
                        Flows.resetFailedFlows(flow);
                        this.getApplication().getScheduler().scheduleNow(flow);

                        addMessage(req, String.format("Flow[%s] restarted.", id));
                    }
                }
                catch (NumberFormatException e) {
                    addMessage(req, String.format("Apparently [%s] is not a valid long.", getParam(req, "id")));
                }
            }
        }

        long currMaxId = allFlows.getCurrMaxId();

        int size = 25;
        String sizeParam = req.getParameter("size");
        if(sizeParam != null)
            size = Integer.parseInt(sizeParam);

        List<ExecutableFlow> execs = new ArrayList<ExecutableFlow>(size);
        for (int i = 0; i < size; ++i) {
            ExecutableFlow flow = allFlows.loadExecutableFlow(currMaxId - i);

            if (flow != null) {
                execs.add(flow);
            }
        }

        Page page = newPage(req, resp, "azkaban/web/pages/execution_history.vm");
        page.add("executions", execs);
        page.render();
    }

}
