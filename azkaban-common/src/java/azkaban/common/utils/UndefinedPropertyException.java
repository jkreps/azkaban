package azkaban.common.utils;

/**
 * Indicates that a required property is missing from the Props
 * 
 * @author jkreps
 * 
 */
public class UndefinedPropertyException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public UndefinedPropertyException(String message) {
        super(message);
    }

}
