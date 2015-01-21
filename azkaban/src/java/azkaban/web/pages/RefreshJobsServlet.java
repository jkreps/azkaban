/*
 * Copyright 2011 Adconion, Inc.
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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import azkaban.app.JobManager;
import azkaban.common.web.Page;
import azkaban.web.AbstractAzkabanServlet;

public class RefreshJobsServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;
    private static Logger logger = Logger.getLogger(RefreshJobsServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        renderPage(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        String confirmed = request.getParameter("confirm");
        if (confirmed != null && confirmed.equals("true")) {
            try {
                getApplication().reloadJobsFromDisk();
                addMessage(request, "Jobs have been refreshed");
            }
            catch(RuntimeException e) {
                addError(request, "An exception occurred when refreshing jobs: " + e.toString());
                addError(request, "See the log for errors");
                logger.error("Failed to refresh jobs", e);
            }
        }
        else {
            addError(request, "Jobs not refreshed - no confirmation given");
        }
        renderPage(request, response);
    }
    
    private void renderPage(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        Page page = newPage(request, response, "azkaban/web/pages/refresh_jobs.vm");
        page.render();
    }
}
