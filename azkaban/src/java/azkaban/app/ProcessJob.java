package azkaban.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

/**
 * A job that runs a simple unix command
 * 
 * @author jkreps
 * 
 */
public class ProcessJob extends AbstractJob {

    public static final String ENV_PREFIX = "env.";
    private final Props _props;
    private volatile Process _process;
    private volatile boolean _isComplete;

    public ProcessJob(JobDescriptor descriptor) {
        super(descriptor.getId());
        this._props = descriptor.getProps();
        this._isComplete = false;
    }

    public void run() {
        List<String> commands = new ArrayList<String>();
        commands.add(_props.getString("command"));
        for(int i = 1; _props.containsKey("command." + i); i++)
            commands.add(_props.getString("command." + i));

        info(commands.size() + " commands to execute.");

        Set<String> keys = _props.keySet();

        Map<String, String> env = new HashMap<String, String>();
        for(String key: keys) {
            if(key.toLowerCase().startsWith(ENV_PREFIX)) {
                String value = _props.getString(key);
                String strippedKey = key.substring(ENV_PREFIX.length());
                env.put(strippedKey, value);
                info("Setting environment Variable: " + strippedKey + "=" + value);
            }
        }

        for(String command: commands) {
            info("Executing command: " + command);
            String[] cmdPieces = command.split("\\s+");
            ProcessBuilder builder = new ProcessBuilder(cmdPieces);
            String cwd = _props.containsKey("working.dir") ? _props.get("working.dir")
                                                          : System.getProperty("user.dir");
            builder.directory(new File(cwd));
            builder.environment().putAll(env);

            try {
                _process = builder.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Thread outputGobbler = new LoggingGobbler(new InputStreamReader(_process.getInputStream()),
                                                      Level.INFO);
            Thread errorGobbler = new LoggingGobbler(new InputStreamReader(_process.getErrorStream()),
                                                     Level.ERROR);
            outputGobbler.start();
            errorGobbler.start();
            int exitCode = 0;
            try {
                exitCode = _process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            _isComplete = true;
            if(exitCode != 0)
                throw new RuntimeException("Processes ended with exit code " + exitCode + ".");

            // try to wait for everything to get logged out before exiting
            try {
                outputGobbler.join(1000);
                errorGobbler.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void cancel() throws Exception {
        if(_process != null)
            _process.destroy();
    }

    @Override
    public double getProgress() {
        return _isComplete ? 1.0 : 0.0;
    }

    private class LoggingGobbler extends Thread {

        private final BufferedReader _inputReader;
        private final Level _loggingLevel;

        public LoggingGobbler(InputStreamReader inputReader, Level level) {
            _inputReader = new BufferedReader(inputReader);
            _loggingLevel = level;
        }

        @Override
        public void run() {
            try {
                while(!Thread.currentThread().isInterrupted()) {
                    String line = _inputReader.readLine();
                    if(line == null)
                        return;
                    getLog().log(_loggingLevel, line);
                }
            } catch(IOException e) {
                error("Error reading from logging stream:", e);
            }
        }
    }

    public Props getProps() {
        return _props;
    }
}
