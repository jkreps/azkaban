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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A servlet that reads log files from the filesystem
 * 
 * @author jkreps
 * 
 */
public class LogServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;

    private String _logDir;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        resp.setContentType("text/plain");
        String file = getParam(req, "file");

        String fileName = _logDir + File.separator + file;
        if(!new File(fileName).canRead()) {
            resp.getWriter().write("No log available at '" + file + "'");
            return;
        }

        long size = new File(fileName).length();
        FileInputStream f = new FileInputStream(fileName);
        long skipped = 0;
        if(req.getParameter("full") == null) {
            skipped = Math.max(0, size - 200 * 1024);
            f.skip(skipped);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(f));
        try {
            Writer writer = resp.getWriter();
            String line = reader.readLine();
            if(skipped > 0) {
                // skip to next full line
                writer.write("Skipping "
                             + skipped
                             + " bytes. Use the optional parameter ?full to see the entire log.\n\n");
                line = reader.readLine();
            }
            for(; line != null; line = reader.readLine()) {
                writer.write(line);
                writer.write("\n");
            }
        } finally {
            reader.close();
        }
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        _logDir = WebUtils.getApp(config).getLogDirectory();
    }

}
