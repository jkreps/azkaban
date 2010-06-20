package azkaban.util.process;

public class ProcessFailureException extends RuntimeException {

	private static final long serialVersionUID = 1;
	
	private final int exitCode;
	
	public ProcessFailureException(int exitCode) {
		this.exitCode = exitCode;
	}

	public int getExitCode() {
		return exitCode;
	}
	
}
