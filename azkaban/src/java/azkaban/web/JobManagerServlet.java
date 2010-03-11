package azkaban.web;

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

/**
 * Deploy and undeploy jobs
 * 
 * @author jkreps
 * 
 */
public class JobManagerServlet extends AbstractAzkabanServlet {

    private static final long serialVersionUID = 1;
    private static final int DEFAULT_UPLOAD_DISK_SPOOL_SIZE = 20 * 1024 * 1024;
    
    private JobManager _jobManager;
    private MultipartParser _multipartParser;
    private String _tempDir;
    
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        this._jobManager = this.getApplication().getJobManager();
        this._multipartParser = new MultipartParser(DEFAULT_UPLOAD_DISK_SPOOL_SIZE);
        
        _tempDir = this.getApplication().getTempDirectory();
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String method = request.getMethod().toLowerCase();
        if(method.equals("get")) {
            response.getWriter().write("Hello!");
        } else if(method.equals("put") || method.equals("post")) {
            if(!ServletFileUpload.isMultipartContent(request))
                throw new ServletException("No job file found!");
            Map<String, Object> params = this._multipartParser.parseMultipart(request);
            try {
                FileItem item = (FileItem) params.get("file");
                String deployPath = (String) params.get("path");
                File jobDir = unzipFile(item);
                this._jobManager.deployJobDir(jobDir.getAbsolutePath(), deployPath);
            } catch (Exception e) {
                String redirectError = (String)params.get("redirect_error");
                setMessagedUrl(response, redirectError, "Installation Failed: " + e.getLocalizedMessage());

                return;
            }
            
            String redirectSuccess = (String)params.get("redirect_success");
            setMessagedUrl(response, redirectSuccess, "Installation Succeeded");
        } else if(method.equals("delete")) {}
        
       
    }

    private void setMessagedUrl(HttpServletResponse response, String redirectUrl, String message) throws IOException {
        String url = redirectUrl + "/" + message;
        response.sendRedirect(response.encodeRedirectUrl(url));
    }
    
    @SuppressWarnings("unchecked")
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
