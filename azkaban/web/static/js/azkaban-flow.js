var svgns   = "http://www.w3.org/2000/svg";
var xlinkns ="http://www.w3.org/1999/xlink";

var currentGraph;
var svgElement;

var editButton;
var buttonRow;
var currentSelected;
var editMode = false;


function cancelEvent(e) {
	e = e ? e : window.event;
	if (e.stopPropagation()) {
		e.stopPropagation();
	}
	if (e.preventDefault) {
		e.preventDefault();
	}
	e.cancel = true;
	e.returnValue = false;
	return false;
}

function toggleEdit() {
	if (editMode) {
		editMode = false;
		setEditMode(editMode);
	}
	else {
		editMode = true;
		setEditMode(editMode);
	}		
}

function loadGraph() {
	var svgGraph = new SVGGraph();
	svgGraph.initGraph();
	svgGraph.mapNodeTypeToColor("normal", "#000000");
	svgGraph.mapNodeTypeToColor("disabled", "#000000");
	svgGraph.mapNodeTypeToColor("ready", "#000000");
	svgGraph.mapNodeTypeToColor("completed", "#000000");	
	svgGraph.mapNodeTypeToColor("succeeded", "#006600");
	svgGraph.mapNodeTypeToColor("failed", "#CC1108");
	svgGraph.mapEdgeTypeToColor("running", "#0000FF");
	return svgGraph;
}

function resetTransform() {
	if (currentGraph) {
		var content = document.getElementById("content2");
		currentGraph.resetTransform(content.clientWidth,content.clientHeight);
	}
}

function attachGraph(svgGraph) {
	if (currentGraph) {
		currentGraph.dettachGraph();
	}

	currentGraph = svgGraph;
	svgGraph.attachGraph(svgElement);
	setEditMode(false);
}

var isMouseDown = false;
var mouseX;
var mouseY;
var moveNode = false;
function moveGraph(element, evt, type) {
	if (!evt) var evt = window.event;

	if (type == "move") {
    	if (isMouseDown) {
			var deltaX = evt.clientX - mouseX;
			var deltaY = evt.clientY - mouseY;
			if (moveNode) {
				currentGraph.moveSelectedNodes(deltaX, deltaY);
			}
			else {
				currentGraph.panGraph(deltaX, deltaY);
			}
			mouseX = evt.clientX;
			mouseY = evt.clientY;
		}
	}
	else if (type == "down") {
		moveNode = shouldMoveNode();
		isMouseDown = true;
		element.setAttribute('onmousemove',"moveGraph(this, event, 'move')");
		mouseX = evt.clientX;
		mouseY = evt.clientY;
	}
   else if (type == "up") {
		isMouseDown = false;
		element.setAttribute('onmousemove',null);
	}

}

function shouldMoveNode() {
	if (!currentGraph) {
		return false;
	}

	return editMode && currentGraph.getSelectedNodes() && currentGraph.getCursorNode();
}

var mouseInside = false;
function mouseFocus(type) {
	mouseInside = (type == 'true');
}

function zoomGraph(evt) {
	if (!mouseInside || !currentGraph) {
		return;
	}
	if (!evt) var evt = window.event;
	
	var delta = 0;
	if (evt.wheelDelta) {
		delta =event.wheelDelta / 120;
	}
	else if (evt.detail) {
		delta = -evt.detail / 3;
	}
	
	var scale = 1;
	if ( delta > 0 ) {
		for (var i = 0; i < delta; ++i) {
			scale *= 1.01;
		}
	}
	else {
		for (var i = 0; i < -delta; ++i) {
			scale *= 0.99;
		}
	}

	var content = document.getElementById("content2");
	var x = evt.pageX - content.offsetLeft;
	var y = evt.pageY - content.offsetTop;
	
	currentGraph.zoomGraph(scale, x, y);	
	
	return cancelEvent(evt);
}

function setEditMode(mode) {
	if (mode) {
		editButton.setAttributeNS(xlinkns, 'xlink:href', contextURL + '/static/editunlock.png');	
	}
	else {
		editButton.setAttributeNS(xlinkns, 'xlink:href', contextURL + '/static/editlock.png');
	}
	editMode = mode;
}

function setupEditButton(svgElement) {
	buttonRow = document.createElementNS(svgns, 'g');
	buttonRow.setAttribute('id', 'postgraph');
	editButton = document.createElementNS(svgns, 'image');
	editButton.setAttribute("x", "0px");
	editButton.setAttribute("y", "0px");
	editButton.setAttribute("width", "32px");
	editButton.setAttribute("height", "32px");
	editButton.setAttribute("opacity", "0.5");	
	editButton.setAttributeNS(xlinkns, "xlink:href", contextURL + "/static/edit-lock.png");				
	editButton.setAttribute("onmouseover", "this.setAttribute('opacity','1.0')");
	editButton.setAttribute("onmouseout","this.setAttribute('opacity','0.5')");
	editButton.setAttribute("onclick", "toggleEdit()");

	buttonRow.appendChild(editButton);
	buttonRow.setAttribute("transform", "translate(3, 470)");
	
	svgElement.appendChild(buttonRow);
}

function loadFlow(flowdata) {
	var svgGraph = loadGraph(); 
	attachGraph(svgGraph);
	var flow = jQuery.parseJSON(flowdata);
	for(var i=0; i < flow.nodes.length; i++) {
		var flowNode = flow.nodes[i];
		if (flowNode != null) {
			var node = svgGraph.createNode(flowNode.name, flowNode.name, flowNode.x, flowNode.y, flowNode.status);
			if (flowNode.status == "succeeded" || flowNode.status == "completed") {
				currentGraph.setEnabledNode(node, false);
			}
			
			$(node).contextMenu({
				menu: 'nodeMenu'
			},
			function(action, el, pos) {

					if (action == 'disable' ) {
						currentGraph.setEnabled(false);
					}
					else if (action == 'enable') {
						currentGraph.setEnabled(true);
					}
			});
		}
	}
	
	for (var i=0; i < flow.dependencies.length; i++) {
		var edge = flow.dependencies[i];
		svgGraph.createEdge(edge.dependency, edge.dependent, edge.status);
	}
	
	resetTransform();
}

$(function () {
	svgElement = document.getElementById("graph");
	setupEditButton(svgElement);
	loadFlow(flowData);
	$("#executeButton").button();
});

function executeFlow() {
	var retval = currentGraph.getDisabledNodeValues();
	jQuery.ajax( {
		'type': 'POST',
		'url': contextURL + "/flow", 
		'async':false,
		'data': {
			"id":flowID,
			"name": name,
			"action" : action,
			"disabled" : retval
		},
		'success': function(data) {
			window.location = contextURL + "/";
		}
	});

}

window.onload = function() {
	if (window.addEventListener) document.getElementById("content2").addEventListener('DOMMouseScroll', zoomGraph, false);
	document.getElementById("content2").onmousewheel = zoomGraph;
}
