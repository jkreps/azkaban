package azkaban.app;

public class AppConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public AppConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AppConfigurationException(String message) {
        super(message);
    }

    public AppConfigurationException(Throwable cause) {
        super(cause);
    }

}
