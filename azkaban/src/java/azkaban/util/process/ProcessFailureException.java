package azkaban.util.process;

public class ProcessFailureException extends RuntimeException {

	private static final long serialVersionUID = 1;
	
	private final int exitCode;
	private final String logSnippet;
	
	public ProcessFailureException(int exitCode, String logSnippet) {
		this.exitCode = exitCode;
		this.logSnippet = logSnippet;
	}

	public int getExitCode() {
		return exitCode;
	}
	
	public String getLogSnippet() {
	    return this.logSnippet;
	}
	
}
