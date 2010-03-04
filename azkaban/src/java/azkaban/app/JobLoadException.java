package azkaban.app;

/**
 * An exception thrown when we are unable to load a job descriptor
 * 
 * @author jkreps
 * 
 */
public class JobLoadException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public JobLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobLoadException(String message) {
        super(message);
    }

    public JobLoadException(Throwable cause) {
        super(cause);
    }

}
