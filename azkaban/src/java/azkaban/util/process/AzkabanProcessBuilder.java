package azkaban.util.process;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

/**
 * Helper code for building a process
 * 
 * @author jkreps
 */
public class AzkabanProcessBuilder {

    private List<String> cmd = new ArrayList<String>();
    private Map<String, String> env = new HashMap<String, String>();
    private String workingDir = System.getProperty("user.dir");
    private Logger logger = Logger.getLogger(AzkabanProcess.class);
    private int stdErrSnippetSize = 30;
    private int stdOutSnippetSize = 30;
    
    public AzkabanProcessBuilder(String...command) {
        addArg(command);
    }
    
    public AzkabanProcessBuilder addArg(String...command) {
        for(String c: command)
            cmd.add(c);
        return this;
    }
    
    public AzkabanProcessBuilder setWorkingDir(String dir) {
        this.workingDir = dir;
        return this;
    }
    
    public AzkabanProcessBuilder setWorkingDir(File f) {
        return setWorkingDir(f.getAbsolutePath());
    }
    
    public String getWorkingDir() {
        return this.workingDir;
    }
    
    public AzkabanProcessBuilder addEnv(String variable, String value) {
        env.put(variable, value);
        return this;
    }
    
    public AzkabanProcessBuilder setEnv(Map<String, String> m) {
        this.env = m;
        return this;
    }
    
    public Map<String, String> getEnv() {
        return this.env;
    }
    
    public AzkabanProcessBuilder setStdErrorSnippetSize(int size) {
        this.stdErrSnippetSize = size;
        return this;
    }
    
    public AzkabanProcessBuilder setStdOutSnippetSize(int size) {
        this.stdOutSnippetSize = size;
        return this;
    }
    
    public AzkabanProcessBuilder setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }
    
    public AzkabanProcess build() {
        return new AzkabanProcess(cmd, env, workingDir, logger);
    }
    
    public List<String> getCommand() {
        return this.cmd;
    }
    
    public String getCommandString() {
        return Joiner.on(" ").join(getCommand());
    }
    
    @Override
    public String toString() {
        return "ProcessBuilder(cmd = " + Joiner.on(" ").join(cmd) + ", env = " + env + ", cwd = " + workingDir + ")";
    }
}
