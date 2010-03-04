package azkaban.app;

/**
 * Indicates an error when deploying a job
 * 
 * @author jkreps
 * 
 */
public class JobDeploymentException extends RuntimeException {

    private final static long serialVersionUID = 1;

    public JobDeploymentException(String message) {
        super(message);
    }

    public JobDeploymentException(Throwable cause) {
        super(cause);
    }

    public JobDeploymentException(String message, Throwable cause) {
        super(message, cause);
    }

}
