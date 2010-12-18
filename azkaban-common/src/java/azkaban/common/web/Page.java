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

package azkaban.common.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import azkaban.common.utils.Utils;

/**
 * A page to display
 * 
 * @author jkreps
 * 
 */
public class Page {

    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final VelocityEngine engine;
    private final VelocityContext context;
    private final String template;
    private final GuiUtils utils = new GuiUtils();

    public Page(HttpServletRequest request,
                HttpServletResponse response,
                VelocityEngine engine,
                String template) {
        this.request = Utils.nonNull(request);
        this.response = Utils.nonNull(response);
        this.engine = Utils.nonNull(engine);
        this.template = Utils.nonNull(template);
        this.context = new VelocityContext();
        this.context.put("utils", utils);
        this.context.put("session", request.getSession(true));
        this.context.put("context", request.getContextPath());
    }

    public void render() {
        try {
            engine.mergeTemplate(template, "UTF-8", context, response.getWriter());
        } catch(Exception e) {
            throw new PageRenderException(e);
        }
    }

    public void add(String name, Object value) {
        context.put(name, value);
    }
}
