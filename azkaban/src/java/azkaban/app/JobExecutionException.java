package azkaban.app;

public class JobExecutionException extends RuntimeException {

    private final static long serialVersionUID = 1;

    public JobExecutionException(String message) {
        super(message);
    }

    public JobExecutionException(Throwable cause) {
        super(cause);
    }

    public JobExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

}