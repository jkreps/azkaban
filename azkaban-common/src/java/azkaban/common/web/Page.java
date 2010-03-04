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
            engine.mergeTemplate(template, context, response.getWriter());
        } catch(Exception e) {
            throw new PageRenderException(e);
        }
    }

    public void add(String name, Object value) {
        context.put(name, value);
    }
}
