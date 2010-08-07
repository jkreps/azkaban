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

package azkaban.web;

import azkaban.app.AzkabanApplication;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import azkaban.common.web.Page;

/**
 * Base Servlet for pages
 * 
 * @author jkreps
 * 
 */
public class AbstractAzkabanServlet extends HttpServlet {

    private static final long serialVersionUID = 1;
    private static final String AZKABAN_NAME = "instancename";
    
    private AzkabanApplication _app;

    public AzkabanApplication getApplication() {
        return _app;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        _app = WebUtils.getApp(config);
    }

    public boolean hasParam(HttpServletRequest request, String param) {
        return request.getParameter(param) != null;
    }

    public String getParam(HttpServletRequest request, String name) throws ServletException {
        String p = request.getParameter(name);
        if(p == null || p.equals(""))
            throw new ServletException("Missing required parameter '" + name + "'.");
        else
            return p;
    }

    public int getIntParam(HttpServletRequest request, String name) throws ServletException {
        String p = getParam(request, name);
        return Integer.parseInt(p);
    }

    protected void setSessionValue(HttpServletRequest request, String key, Object value) {
        request.getSession(true).setAttribute(key, value);
    }

    @SuppressWarnings("unchecked")
    protected void addSessionValue(HttpServletRequest request, String key, Object value) {
        List l = (List) request.getSession(true).getAttribute(key);
        if(l == null)
            l = new ArrayList();
        l.add(value);
        request.getSession(true).setAttribute(key, l);
    }

    protected void addError(HttpServletRequest request, String message) {
        addSessionValue(request, "errors", message);
    }

    protected void addMessage(HttpServletRequest request, String message) {
        addSessionValue(request, "messages", message);
    }

    protected Page newPage(HttpServletRequest req, HttpServletResponse resp, String template) {
        Page page = new Page(req, resp, _app.getVelocityEngine(), template);
        page.add(AZKABAN_NAME, _app.getAppInstanceName());
        
        return page;
    }

    /**
     * set runtime properties
     * 
     * @param req
     * @param resp
     */
    protected void setRuntimeProperties(HttpServletRequest req, HttpServletResponse resp) {
        
        if(_app != null &&  _app.getRuntimeProperty(azkaban.app.AppCommon.LOG_URL_PREFIX) == null ) {  
            StringBuffer requestUrl = req.getRequestURL();
            requestUrl.append("logs?file=");
             _app.setRuntimeProperty (azkaban.app.AppCommon.LOG_URL_PREFIX, requestUrl.toString());
        }
    }

}
