package azkaban.web.pages;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.filefilter.RegexFileFilter;

import azkaban.common.web.Page;
import azkaban.web.AbstractAzkabanServlet;

public class JobUploadServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        Page page = new Page(req,
                             resp,
                             getApplication().getVelocityEngine(),
                             "azkaban/web/pages/upload_jobs.vm");
        
        String fsPath = req.getPathInfo();
        if (fsPath != null && fsPath.length() > 1) {
            this.addMessage(req, fsPath.substring(1));
        }
        
        page.render();
    }

}
