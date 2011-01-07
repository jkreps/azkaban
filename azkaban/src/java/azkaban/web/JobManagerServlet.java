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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.ZipFile;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;

import azkaban.app.JobManager;
import azkaban.common.utils.Utils;
import org.apache.log4j.Logger;

/**
 * Deploy and undeploy jobs
 * 
 * @author jkreps
 * 
 */
public class JobManagerServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;
    private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;

    private static final Logger log = Logger.getLogger(JobManagerServlet.class);
    
    private MultipartParser _multipartParser;
    private String _tempDir;
    
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        this._multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
        
        _tempDir = this.getApplication().getTempDirectory();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        response.getWriter().write("Hello!");
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        doPut(request, response);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        if(!ServletFileUpload.isMultipartContent(request))
            throw new ServletException("No job file found!");

        Map<String, Object> params = this._multipartParser.parseMultipart(request);

        try {
            final AzkabanApplication app = getApplication();
            final JobManager jobManager = app.getJobManager();

            FileItem item = (FileItem) params.get("file");
            String deployPath = (String) params.get("path");
            File jobDir = unzipFile(item);

            jobManager.deployJobDir(jobDir.getAbsolutePath(), deployPath);
        } catch (Exception e) {
            log.info("Installation Failed.", e);
            String redirectError = (String)params.get("redirect_error");
            setMessagedUrl(response, redirectError, "Installation Failed: " + e.getLocalizedMessage());

            return;
        }

        String redirectSuccess = (String)params.get("redirect_success");
        setMessagedUrl(response, redirectSuccess, "Installation Succeeded");
    }

    private void setMessagedUrl(HttpServletResponse response, String redirectUrl, String message) throws IOException {
        String url = redirectUrl + "/" + message;
        response.sendRedirect(response.encodeRedirectURL(url));
    }
    
    private File unzipFile(FileItem item) throws ServletException, IOException {
        File temp = File.createTempFile("job-temp", ".zip");
        temp.deleteOnExit();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(temp));
        IOUtils.copy(item.getInputStream(), out);
        out.close();
        ZipFile zipfile = new ZipFile(temp);
        File unzipped = Utils.createTempDir(new File(_tempDir));
        Utils.unzip(zipfile, unzipped);
        temp.delete();
        return unzipped;
    }

}
