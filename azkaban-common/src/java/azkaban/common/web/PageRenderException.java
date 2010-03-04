package azkaban.common.web;

/**
 * Thrown if there is an error rendering the page
 * 
 * @author jkreps
 * 
 */
public class PageRenderException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public PageRenderException(Throwable cause) {
        super(cause);
    }
}
