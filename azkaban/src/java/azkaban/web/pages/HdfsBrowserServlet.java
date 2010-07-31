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

package azkaban.web.pages;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import azkaban.common.web.HdfsAvroFileViewer;
import azkaban.common.web.HdfsFileViewer;
import azkaban.common.web.JsonSequenceFileViewer;
import azkaban.common.web.Page;
import azkaban.common.web.TextFileViewer;
import azkaban.web.AbstractAzkabanServlet;
import azkaban.web.WebUtils;

/**
 * A servlet that shows the filesystem contents
 * 
 * @author jkreps
 * 
 */
public class HdfsBrowserServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;

    private FileSystem _fs;

    private ArrayList<HdfsFileViewer> _viewers = new ArrayList<HdfsFileViewer>();

    // Default viewer will be a text viewer
    private HdfsFileViewer _defaultViewer = new TextFileViewer();

    private static Logger logger = Logger.getLogger(HdfsBrowserServlet.class);

    public HdfsBrowserServlet() {
        super();
        _viewers.add(new HdfsAvroFileViewer());
        _viewers.add(new JsonSequenceFileViewer());
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        try {
            Configuration conf = new Configuration();
            conf.setClassLoader(this.getApplication().getClassLoader());
            _fs = FileSystem.get(conf);
        } catch(IOException e) {
            throw new ServletException(e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {

        String prefix = req.getContextPath() + req.getServletPath();
        String fsPath = req.getRequestURI().substring(prefix.length());
        if(fsPath.length() == 0)
            fsPath = "/";

        if(logger.isDebugEnabled())
            logger.debug("path=" + fsPath);

        Path path = new Path(fsPath);
        if(!_fs.exists(path))
            throw new IllegalArgumentException(path.toUri().getPath() + " does not exist.");
        else if(_fs.isFile(path))
            displayFile(req, resp, path);
        else if(_fs.getFileStatus(path).isDir())
            displayDir(req, resp, path);
        else
            throw new IllegalStateException("It exists, it is not a file, and it is not a directory, what is it precious?");

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String action = req.getParameter("action");
        if("delete".equals(action)) {
            Path theFile = new Path(req.getParameter("file"));
            _fs.delete(theFile, true);
        } else {
            throw new ServletException("Unknown action '" + action + "'!");
        }

    }

    private void displayDir(HttpServletRequest req, HttpServletResponse resp, Path path)
            throws IOException {

        Page page = newPage(req, resp, "azkaban/web/pages/hdfs_browser_dir.vm");

        List<Path> paths = new ArrayList<Path>();
        List<String> segments = new ArrayList<String>();
        Path curr = path;
        while(curr.getParent() != null) {
            paths.add(curr);
            segments.add(curr.getName());
            curr = curr.getParent();
        }

        Collections.reverse(paths);
        Collections.reverse(segments);

        page.add("paths", paths);
        page.add("segments", segments);
        page.add("subdirs", _fs.listStatus(path)); // ??? line
        page.render();

    }

    private void displayFile(HttpServletRequest req, HttpServletResponse resp, Path path)
            throws IOException {

        int startLine = WebUtils.getInt(req, "start_line", 1);
        int endLine = WebUtils.getInt(req, "end_line", 1000);

        // use registered viewers to show the file content
        boolean outputed = false;
        OutputStream output = resp.getOutputStream();
        for(HdfsFileViewer viewer: _viewers) {
            if(viewer.canReadFile(_fs, path)) {
                viewer.displayFile(_fs, path, output, startLine, endLine);
                outputed = true;
                break; // don't need to try other viewers
            }
        }

        // use default text viewer
        if(!outputed) {
            if(_defaultViewer.canReadFile(_fs, path)) {
                _defaultViewer.displayFile(_fs, path, output, startLine, endLine);
            } else {
                output.write(("Sorry, no viewer available for this file. ").getBytes("UTF-8"));
            }
        }
    }
}
