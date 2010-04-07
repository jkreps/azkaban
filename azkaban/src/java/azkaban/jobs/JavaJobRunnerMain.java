package azkaban.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.ConsoleAppender;

public class JavaJobRunnerMain {
    public static final String JOB_CLASS = "job.class";
    public static final String DEFAULT_RUN_METHOD = "run";
    public static final String RUN_METHOD_PARAM = "method.run";
    public static final String PROPS_CLASS = "azkaban.common.utils.Props";
    
    private static final Layout DEFAULT_LAYOUT = new PatternLayout("%p %m\n");
    
    public static void main(String[] args) throws Exception {
        JavaJobRunnerMain wrapper = new JavaJobRunnerMain();
    }
    
    public JavaJobRunnerMain() throws Exception {
        String jobName = System.getenv(ProcessJob.JOB_NAME_ENV);
        String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
        
        Logger logger = Logger.getRootLogger();
        logger.removeAllAppenders();
        ConsoleAppender appender = new ConsoleAppender(DEFAULT_LAYOUT);
        appender.activateOptions();
        logger.addAppender(appender);
        
        Properties prop = new Properties();
        prop.load(new BufferedReader(new FileReader(propsFile)));

        logger.info("Running job " + jobName);
        String className = prop.getProperty(JOB_CLASS);
        if (className == null) {
            throw new Exception("Class name is not set.");
        }
        logger.info("Class name " + className);
        
        // Create the object.
        Object obj = getObject(jobName, className, prop);
        if (obj == null) {
            throw new Exception("Could not create running object");
        }

        String runMethod = prop.getProperty(RUN_METHOD_PARAM, DEFAULT_RUN_METHOD);        
        logger.info("Invoking method " + runMethod);
        obj.getClass().getMethod(runMethod, new Class<?>[]{}).invoke( obj, new Object[]{});
    }
    
    private static Object getObject(String jobName, String className, Properties properties) throws Exception {        
        Class<?> runningClass = JavaJobRunnerMain.class.getClassLoader().loadClass(className);
        
        if (runningClass == null) {
            throw new Exception("Class " + className + " was not found. Cannot run job.");
        }
        
        // Looks for azkaban.common.utils.Props if it exists. If it doesn't, then it tries the other constructors.
        Class<?> propsClass = null; 
        try {
            propsClass = JavaJobRunnerMain.class.getClassLoader().loadClass(PROPS_CLASS);
        }
        catch (ClassNotFoundException e) {    
        }
        
        Object obj = null;
        if(propsClass != null && getConstructor(runningClass, String.class, propsClass) != null) {
            // This case covers the use of azkaban.common.utils.Props with the
            Constructor<?> con = getConstructor(propsClass, propsClass, Properties[].class);
            Object props = con.newInstance(null, new Properties[] { properties });
            obj = getConstructor(runningClass, String.class, propsClass).newInstance(jobName, props);
        }
        else if (getConstructor(runningClass, String.class, Properties.class) != null) {
            obj = getConstructor(runningClass, String.class, Properties.class).newInstance(jobName, properties);
        }
        else if (getConstructor(runningClass, String.class) != null) {
            obj = getConstructor(runningClass, String.class).newInstance(jobName);
        }
        else if (getConstructor(runningClass) != null) {
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
