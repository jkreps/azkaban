package azkaban.workflow.flow;

public class Dependency {
	private final FlowNode dependent;
	private final FlowNode dependency;
	
	public Dependency(FlowNode dependency, FlowNode dependent) {
		this.dependent = dependent;
		this.dependency = dependency;
	}

	public FlowNode getDependent() {
		return dependent;
	}

	public FlowNode getDependency() {
		return dependency;
	}
}