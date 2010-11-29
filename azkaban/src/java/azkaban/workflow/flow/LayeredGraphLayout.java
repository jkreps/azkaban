/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.workflow.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import azkaban.workflow.Flow;
import azkaban.workflow.flow.FlowNode;

public class LayeredGraphLayout extends DagLayout {
	ArrayList<ArrayList<Node>> levelsList = new ArrayList<ArrayList<Node>>();
	LinkedHashMap<String, JobNode> nodesMap = new LinkedHashMap<String, JobNode>();
	
	public static final float LEVEL_HEIGHT = 120;
	public static final float LEVEL_WIDTH = 100;
	public static final float LEVEL_WIDTH_ADJUSTMENT = 5;
	
	public LayeredGraphLayout(Flow flow) {
		super(flow);
	}

	private void assignLevels() {
		int longestWord = 0;
		levelsList.clear();
		
		// Assign each node the # of dependents it has.
		LinkedList<JobNode> bottomOfThePile = new LinkedList<JobNode>();
		for (FlowNode flowNode : flow.getFlowNodes()) {
			longestWord = Math.max(flowNode.getAlias().length(), longestWord);
			JobNode jobNode = new JobNode(flowNode);
			jobNode.setCounter(flowNode.getDependents().size());
			
			nodesMap.put(flowNode.getAlias(), jobNode);
			if (jobNode.getCounter() == 0) {
				bottomOfThePile.add(jobNode);
			}
		}
		
		// Run breath first search and assign levels.
		while(bottomOfThePile.size() > 0) {
			JobNode jobNode = bottomOfThePile.remove();
			
			int level = jobNode.getLevel();
			//Add to list
			ArrayList<Node> levels = null;
			if (levelsList.size() == level) {
				levels = new ArrayList<Node>();
				levelsList.add(levels);
			}
			else {
				levels = levelsList.get(level);
			}
			levels.add(jobNode);
			
			int nextLevel = level + 1;
			FlowNode flow = jobNode.getFlowNode();
			for(String str : flow.getDependencies()) {
				JobNode nextNode = nodesMap.get(str);
				int max = Math.max(nextNode.getLevel(), nextLevel);
				nextNode.setLevel(max);

				int count = nextNode.getCounter();
				count--;
				nextNode.setCounter(count);
				
				// The number of dependents is 0, meaning all dependents were processed.
				if (count == 0) {
					bottomOfThePile.add(nextNode);
				}
			}
			
		}
	}
	
	public void setLayout() {
		nodesMap.clear();
		assignLevels();

		// Adds dummy nodes
		for(JobNode jobNode : nodesMap.values()) {
			layerNodes(jobNode);
		}
		
		setupInitial();
		for (int i = 1; i < levelsList.size(); ++i) {
			ArrayList<Node> level = levelsList.get(i);
			barycenterMethodUncross(level);
			//reorder(level);
		}
		
		for (int i = 0; i < levelsList.size(); ++i) {
			ArrayList<Node> nodeLevel = levelsList.get(i);
			
			float offset = -((float)nodeLevel.size()/2);
			for (Node node: nodeLevel) {
				if (node instanceof JobNode) {
					FlowNode flowNode = ((JobNode) node).getFlowNode();
					flowNode.setPosition(offset*LEVEL_WIDTH, (levelsList.size() - i)*LEVEL_HEIGHT);
				}

				offset += 1;
			}
			
		}

		flow.setLayedOut(true);
	}

	private void setupInitial() {
		
		float maxWidth = 0;
		// Find the level with the largest width
		for (ArrayList<Node> nodeList: levelsList) {
			maxWidth = Math.max(getNodesWidth(nodeList), maxWidth);
		}
		
		// Layout first level evenly.
		float length = 0;
		for (Node node: levelsList.get(0)) {
			node.setPosition(length);
			if (node instanceof JobNode) {
				FlowNode flowNode = ((JobNode) node).getFlowNode();
				int stringLength = flowNode.getAlias().length();
				length+= stringLength * LEVEL_WIDTH_ADJUSTMENT;
			}
			length += LEVEL_WIDTH;
		}
		
		float startingPoint = (maxWidth - length) / 2;
		for (Node node: levelsList.get(0)) {
			node.setPosition(node.getPosition() + startingPoint);
		}
	}
	
	private void reorder(int level) {
		ArrayList<Node> currentLevel = levelsList.get(level);
		
		ArrayList<Node> same = new ArrayList<Node>();
		float startPoint = Float.NEGATIVE_INFINITY;
		float previousPosition = Float.NEGATIVE_INFINITY;
	
		
	}
	
	private void reorder(ArrayList<Node> nodes, int l) {
		int count = 1;
		ArrayList<Node> same = new ArrayList<Node>();
		float startPoint = Float.NEGATIVE_INFINITY;
		float previousPosition = Float.NEGATIVE_INFINITY;
		
		for (int i = 0; i < nodes.size(); ++i) {
			Node node = nodes.get(i);
			float currentPosition = node.getPosition();
			
			if (currentPosition == previousPosition) {
				same.add(node);
			}
			else if (!same.isEmpty()){
				// resolve the unempty.
				
				float width = getNodesWidth(same);
				width += 2* LEVEL_WIDTH;
				float halfWidth = width/2;
				
				
				
				startPoint = previousPosition;
				previousPosition = currentPosition;
				same.clear();
			}
			else {
				startPoint = previousPosition;
				previousPosition = currentPosition;
			}
			
			if (node instanceof JobNode) {
				FlowNode flowNode = ((JobNode) node).getFlowNode();
				int stringLength = flowNode.getAlias().length();
			}
			
			node.setPosition(i);
		}
	}
	
	
	
	private float getNodesWidth(ArrayList<Node> nodes) {
		float length = 0;
		for (int i = 0; i < nodes.size(); ++i) {
			Node node = nodes.get(i);
			if (i == 0) {
				if (nodes.get(i) instanceof JobNode) {
					length += ((JobNode) node).getFlowNode().getAlias().length() * LEVEL_WIDTH_ADJUSTMENT;
				}
			}
			else {
				if (nodes.get(i) instanceof JobNode) {
					length += ((JobNode) node).getFlowNode().getAlias().length() * LEVEL_WIDTH_ADJUSTMENT + LEVEL_WIDTH;
				}
				else {
					length += LEVEL_WIDTH;
				}
			}
		}
		
		return length;
	}
	
	private void barycenterMethodUncross(ArrayList<Node> free) {
		int numOnLevel = free.size();
		for (Node node: free) {
			float average = findAverage(node.getDependents());
			node.setPosition(average);
			node.setNumOnLevel( numOnLevel );
		}
	
		Collections.sort(free);
	}
	
	
	private float findAverage(List<Node> nodes) {
		float sum = 0;
		for (Node node : nodes) {
			sum += node.getPosition();
		}
		
		return sum/(nodes.size());
	}
	
	private void layerNodes(JobNode node) {
		FlowNode flowNode = node.getFlowNode();
		int level = node.getLevel();

		for (String dep : flowNode.getDependencies() ) {
			JobNode depNode = nodesMap.get(dep);
			if (depNode.getLevel() - node.getLevel() > 1) {
				addDummyNodes(node, depNode);
			}
			else {
				node.addDependecy(depNode);
				depNode.addDependent(node);
			}
		}
	}
	
	private void addDummyNodes(JobNode from, JobNode to) {
		int fromLevel = from.getLevel();
		int toLevel = to.getLevel();
		
		Node fromNode = from;
		for (int i = fromLevel+1; i < toLevel; ++i) {
			DummyNode dummyNode = new DummyNode(i);
			
			ArrayList<Node> levelList = levelsList.get(i);
			levelList.add(dummyNode);
			
			dummyNode.addDependecy(fromNode);
			fromNode.addDependent(dummyNode);
			fromNode = dummyNode;
		}
		
		to.addDependecy(from);
		fromNode.addDependent(to);
	}
	
	private class Node implements Comparable {
		private List<Node> dependents = new ArrayList<Node>();
		private List<Node> dependencies = new ArrayList<Node>();
		private float position = 0;
		private int counter = 0;
		private int numOnLevel = 0;
		
		public void addDependent(Node dependent) {
			dependents.add(dependent);
		}
		
		public void addDependecy(Node dependency) {
			dependencies.add(dependency);
		}
		
		public List<Node> getDependencies() {
			return dependencies;
		}
		
		public List<Node> getDependents() {
			return dependents;
		}
		
		public void setPosition(int i) {
			counter = i;
		}
		
		public void setCounter(int counter) {
			this.counter = counter;
		}
		
		public int getCounter() {
			return counter;
		}
		
		public float getPosition() {
			return position;
		}
		
		private void setPosition(float pos) {
			position = pos;
		}

		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			Node other = (Node)arg0;
			Float pos = position;
			
			int comp = pos.compareTo(other.position);
			if ( comp == 0) {
				// Move larger # one to center.
				//int midpos = numOnLevel / 2;
				
				
//				// First priority... # of out nodes.
//				if (this.dependents.size() > other.dependents.size()) {
//					return 1;
//				}
//				else if (this.dependents.size() < other.dependents.size()) {
//					return -1;
//				}
//				
//				// Second priority... # of out nodes
//				if (this.dependencies.size() > other.dependencies.size()) {
//					return 1;
//				}
//				else if (this.dependencies.size() < other.dependencies.size()) {
//					return -1;
//				}
//				
//				if (this instanceof DummyNode) {
//					if (arg0 instanceof DummyNode) {
//						return 0;
//					}
//					return -1;
//				}
//				else if (arg0 instanceof DummyNode){
//					return 1;
//				}
			}
			
			return comp;
		}

		public void setNumOnLevel(int numOnLevel) {
			this.numOnLevel = numOnLevel;
		}

		public int getNumOnLevel() {
			return numOnLevel;
		}
		
	}
	
	private class JobNode extends Node {
		private FlowNode flowNode;
		private int level = 0;
		public JobNode(FlowNode flowNode) {
			this.flowNode = flowNode;
		}
		
		public FlowNode getFlowNode() {
			return flowNode;
		}
		
		public int getLevel() {
			return level;
		}
		
		public void setLevel(int level) {
			this.level=level;
		}
		
	}
	
	private class DummyNode extends Node {
		private int level = 0;
		public DummyNode(int level) {
			this.level = level;
		}
	}
	
	public class LayoutData {
		final FlowNode flowNode;
		public LayoutData(FlowNode flow) {
			flowNode = flow;
		}
	}
}