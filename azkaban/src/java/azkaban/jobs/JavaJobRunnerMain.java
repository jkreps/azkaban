package azkaban.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.ConsoleAppender;

public class JavaJobRunnerMain {

    public static final String JOB_CLASS = "job.class";
    public static final String DEFAULT_RUN_METHOD = "run";
    public static final String DEFAULT_CANCEL_METHOD = "cancel";

    public static final String CANCEL_METHOD_PARAM = "method.cancel";
    public static final String RUN_METHOD_PARAM = "method.run";
    public static final String PROPS_CLASS = "azkaban.common.utils.Props";

    private static final Layout DEFAULT_LAYOUT = new PatternLayout("%p %m\n");

    public final Logger _logger;

    public String _cancelMethod;
    public String _jobName;
    public Object _javaObject;
    private boolean _isFinished = false;
    
    public static void main(String[] args) throws Exception {
        @SuppressWarnings("unused")
        JavaJobRunnerMain wrapper = new JavaJobRunnerMain();
    }

    public JavaJobRunnerMain() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cancelJob();
            }
        }
        );
        
        _jobName = System.getenv(ProcessJob.JOB_NAME_ENV);
        String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);

        _logger = Logger.getRootLogger();
        _logger.removeAllAppenders();
        ConsoleAppender appender = new ConsoleAppender(DEFAULT_LAYOUT);
        appender.activateOptions();
        _logger.addAppender(appender);

        Properties prop = new Properties();
        prop.load(new BufferedReader(new FileReader(propsFile)));

        _logger.info("Running job " + _jobName);
        String className = prop.getProperty(JOB_CLASS);
        if(className == null) {
            throw new Exception("Class name is not set.");
        }
        _logger.info("Class name " + className);

        // Create the object.
        _javaObject = getObject(_jobName, className, prop);
        if(_javaObject == null) {
            throw new Exception("Could not create running object");
        }

        _cancelMethod = prop.getProperty(CANCEL_METHOD_PARAM, DEFAULT_CANCEL_METHOD);
        
        String runMethod = prop.getProperty(RUN_METHOD_PARAM, DEFAULT_RUN_METHOD);
        _logger.info("Invoking method " + runMethod);
        _javaObject.getClass().getMethod(runMethod, new Class<?>[] {}).invoke(_javaObject, new Object[] {});
        _isFinished = true;
    }

    public void cancelJob() {
        if (_isFinished) {
            return;
        }
        _logger.info("Attempting to call cancel on this job");
        if (_javaObject != null) {
            Method method = null;

             try {
                method = _javaObject.getClass().getMethod(_cancelMethod);
            } catch(SecurityException e) {
            } catch(NoSuchMethodException e) {
            }
  
            if (method != null)
                try {
                    method.invoke(_javaObject);
                } catch(Exception e) {
                    if (_logger != null) {
                        _logger.error("Cancel method failed! ", e);
                    }
                }
            else {
                throw new RuntimeException("Job " + _jobName  + " does not have cancel method " + _cancelMethod);
            }
        }
    }

    private static Object getObject(String jobName, String className, Properties properties)
            throws Exception {
        Class<?> runningClass = JavaJobRunnerMain.class.getClassLoader().loadClass(className);

        if(runningClass == null) {
            throw new Exception("Class " + className + " was not found. Cannot run job.");
        }

        // Looks for azkaban.common.utils.Props if it exists. If it doesn't,
        // then it tries the other constructors.
        Class<?> propsClass = null;
        try {
            propsClass = JavaJobRunnerMain.class.getClassLoader().loadClass(PROPS_CLASS);
        } catch(ClassNotFoundException e) {}

        Object obj = null;
        if(propsClass != null && getConstructor(runningClass, String.class, propsClass) != null) {
            // This case covers the use of azkaban.common.utils.Props with the
            Constructor<?> con = getConstructor(propsClass, propsClass, Properties[].class);
            Object props = con.newInstance(null, new Properties[] { properties });
            obj = getConstructor(runningClass, String.class, propsClass).newInstance(jobName, props);
        } else if(getConstructor(runningClass, String.class, Properties.class) != null) {
            obj = getConstructor(runningClass, String.class, Properties.class).newInstance(jobName,
                                                                                           properties);
        } else if(getConstructor(runningClass, String.class) != null) {
            obj = getConstructor(runningClass, String.class).newInstance(jobName);
        } else if(getConstructor(runningClass) != null) {
            obj = getConstructor(runningClass).newInstance();
        }
        return obj;
    }

    private static Constructor<?> getConstructor(Class<?> c, Class<?>... args) {
        try {
            Constructor<?> cons = c.getConstructor(args);
            return cons;
        } catch(NoSuchMethodException e) {
            return null;
        }
    }

}
