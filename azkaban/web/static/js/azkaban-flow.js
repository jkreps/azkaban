var svgns   = "http://www.w3.org/2000/svg";
var xlinkns ="http://www.w3.org/1999/xlink";

var currentGraph;
var svgElement;

var editButton;
var buttonRow;

var editMode = true;
var zoomMode = false;

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
}

var isMouseDown = false;
var mouseX;
var mouseY;
var moveNode = false;
var saveYPos = 0;
function moveGraph(element, evt, type) {
	if (!evt) var evt = window.event;

	if (zoomMode && type == "move") {
		if (!evt) var evt = window.event;
		var deltaY = evt.clientY - mouseY;
		
		moveZoomPicker(deltaY + saveYPos);
	}
	else if (type == "move") {
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
		zoomMode = false;
		saveYPos = parseFloat(zoomPicker.getAttribute("y"));
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
			scale *= 1.05;
		}
	}
	else {
		for (var i = 0; i < -delta; ++i) {
			scale *= 0.95;
		}
	}

	var content = document.getElementById("content2");
	var x = evt.pageX - content.offsetLeft;
	var y = evt.pageY - content.offsetTop;
	
	currentGraph.zoomGraphFactor(scale, x, y);	
	updateZoomPicker();
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

function zoomBarManipulate(mode) {
	zoomMode = mode;
	saveYPos = parseFloat(zoomPicker.getAttribute("y"));
}

function setupEditButton(svgElement) {
	buttonRow = document.createElementNS(svgns, 'g');
	buttonRow.setAttribute('id', 'postgraphButton');
	editButton = document.createElementNS(svgns, 'image');
	editButton.setAttribute("x", "0");
	editButton.setAttribute("y", "0");
	editButton.setAttribute("width", "32px");
	editButton.setAttribute("height", "32px");
	editButton.setAttribute("opacity", "0.5");	
	editButton.setAttributeNS(xlinkns, "xlink:href", contextURL + "/static/editlock.png");				
	editButton.setAttribute("onmouseover", "this.setAttribute('opacity','1.0')");
	editButton.setAttribute("onmouseout","this.setAttribute('opacity','0.5')");
	editButton.setAttribute("onclick", "toggleEdit()");

	buttonRow.appendChild(editButton);
	buttonRow.setAttribute("transform", "translate(3, 470)");
	
	svgElement.appendChild(buttonRow);
}

function clickZoom(zoomFactor) {
	var zoomAmount = 10 * zoomFactor;
	var position = zoomAmount + pickerPos;
	moveZoomPicker(position);
}

function updateZoomPicker() {
	if (currentGraph) {
		pickerPos = zoomHeight - zoomHeight * Math.sqrt(currentGraph.getZoomPercent());
		zoomPicker.setAttribute("y", pickerPos - 5);
	}
}

var pickerPos;
function moveZoomPicker(newpos) {
	pickerPos = newpos;
	if (pickerPos > zoomHeight) {
		pickerPos = zoomHeight;
	}
	else if (pickerPos < 1) {
		pickerPos = 1;
	}
	zoomPicker.setAttribute("y", pickerPos - 5);
	
	if (currentGraph) {
		var percent = 1.0 - (pickerPos / zoomHeight);
		if (percent == 0) {
			percent = 0.0001;
		}
		var content = document.getElementById("content2");
		var py = content.clientHeight/2;
		var px = content.clientWidth/2;
		
		currentGraph.zoomGraphPercent(percent*percent, px, py);
	}
}

function sliderClick(evt, item) {
	var graphTab = document.getElementById("graphTab");
	var newDelta = evt.clientY - graphTab.offsetTop - 25;
	
	moveZoomPicker(newDelta);
}

var zoomBar;
var zoomPicker;
var zoomLine;
var zoomPlus;
var zoomMinus;
var zoomSlider;
var zoomHeight = 200;
var radius = 12;
function setupZoomBar(svgElement) {
	zoomBar = document.createElementNS(svgns, 'g');
	zoomBar.setAttribute('id', 'zoomBar');
	
	// The upper plus button
	zoomPlus = document.createElementNS(svgns, 'g');
	var zoomPlusB = document.createElementNS(svgns, 'circle');
	zoomPlusB.setAttribute("cx", 0);
	zoomPlusB.setAttribute("cy", 0);
	zoomPlusB.setAttribute("r", radius);
	var plusRect1 = document.createElementNS(svgns, 'rect');
	plusRect1.setAttribute("x", -8);
	plusRect1.setAttribute("y", -2);
	plusRect1.setAttribute("width", 16);
	plusRect1.setAttribute("height", 4);
	var plusRect2 = document.createElementNS(svgns, 'rect');
	plusRect2.setAttribute("x", -2);
	plusRect2.setAttribute("y", -8);
	plusRect2.setAttribute("width", 4);
	plusRect2.setAttribute("height", 16);
	zoomPlus.setAttribute("transform", "translate(12.5, 0)");
	zoomPlus.setAttribute("class", "zoomButton");
	zoomPlus.setAttribute("onclick", "clickZoom(-1)");
	zoomPlus.appendChild(zoomPlusB);
	zoomPlus.appendChild(plusRect1);
	zoomPlus.appendChild(plusRect2);
	zoomBar.appendChild(zoomPlus);
	
	// The lower minus button
	zoomMinus = document.createElementNS(svgns, 'g');
	var zoomMinusB = document.createElementNS(svgns, 'circle');
	zoomMinusB.setAttribute("cx",0);
	zoomMinusB.setAttribute("cy",0);
	zoomMinusB.setAttribute("r", radius);
	var minusRect = document.createElementNS(svgns, 'rect');
	minusRect.setAttribute("x", -8);
	minusRect.setAttribute("y", -2);
	minusRect.setAttribute("width", 16);
	minusRect.setAttribute("height", 4);
	zoomMinus.setAttribute("transform", "translate(12.5, " + (200 + radius*2) + " )");
	zoomMinus.setAttribute("class", "zoomButton");
	zoomMinus.setAttribute("onclick", "clickZoom(1)");
	zoomMinus.appendChild(zoomMinusB);
	zoomMinus.appendChild(minusRect);
	zoomBar.appendChild(zoomMinus);
	
	var zoomSlider = document.createElementNS(svgns, 'g');
	zoomSlider.setAttribute("transform", "translate(0, " + radius +")");
	zoomSlider.setAttribute("class", "zoomSlider");
	zoomSlider.setAttribute("onclick", "sliderClick(evt, this)");
	
	zoomLine = document.createElementNS(svgns, 'rect');
	zoomLine.setAttribute("class", "zoomLine");
	zoomLine.setAttribute("x", 8);
	zoomLine.setAttribute("y", 0);
	zoomLine.setAttribute("height", zoomHeight);
	zoomLine.setAttribute("width", 8);
	zoomSlider.appendChild(zoomLine);
	
	var dottedLine = document.createElementNS(svgns, 'line');
	dottedLine.setAttribute("x1", 12.5);
	dottedLine.setAttribute("y1", 0);
	dottedLine.setAttribute("x2", 12.5);
	dottedLine.setAttribute("y2", zoomHeight);
	zoomSlider.appendChild(dottedLine);
	
	// Set up the bar that moves
	zoomPicker = document.createElementNS(svgns, 'rect');
	zoomPicker.setAttribute("rx", 5);
	zoomPicker.setAttribute("class", "zoomPicker");	
	zoomPicker.setAttribute("x", 0);
	zoomPicker.setAttribute("y", 0);
	zoomPicker.setAttribute("width", 24);
	zoomPicker.setAttribute("height", 10);

	zoomPicker.setAttribute("onmousedown", "zoomBarManipulate(true)");
	zoomPicker.setAttribute("onmouseup", "zoomBarManipulate(false)");
	zoomSlider.appendChild(zoomPicker);
	zoomBar.appendChild(zoomSlider);
	
	zoomBar.setAttribute("transform", "translate(4, 14)");
	svgElement.appendChild(zoomBar);
}


function loadFlow(flowdata) {
	var svgGraph = loadGraph(); 
	attachGraph(svgGraph);
	var flow = jQuery.parseJSON(flowdata);
	for(var i=0; i < flow.nodes.length; i++) {
		var flowNode = flow.nodes[i];
		if (flowNode != null) {
			var node = svgGraph.createNode(flowNode.name, flowNode.name, flowNode.x, flowNode.y, flowNode.status);
			if (flowNode.status == "succeeded" || flowNode.status == "completed" || flowNode.status == "disabled") {
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
					else if (action == 'disableAncestor') {
						currentGraph.disableAllAncestors();
					}
					else if (action == 'enableAll') {
						currentGraph.enableAll();
					}
					else if (action == 'jobdetails') {
						var selection = currentGraph.getSelectedNodes()
						if (selection.length > 0) {
							var id = selection[0].id;
							window.location.href= contextURL + "/job?id=" + id;
						}
					}
			});
		}
		
		$(svgGraph).tooltip({
			position: "bottom right",
			offset: [0, 10],
			effect: "fade",
			opacity: 0.9,
			predelay: 500,
			tipClass: flowNode.name + " tooltip",
			onBeforeShow: function(evt, pos) {
				var event = evt;
			},
			onShow: function(evt) {
				var event = evt;
			}
		});
	}
	
	for (var i=0; i < flow.dependencies.length; i++) {
		var edge = flow.dependencies[i];
		svgGraph.createEdge(edge.dependency, edge.dependent, edge.status);
	}
	
	resetTransform();
	updateZoomPicker();

}

$(function () {
	svgElement = document.getElementById("graph");
	setupZoomBar(svgElement);

	loadFlow(flowData);
	$("#executeButton").button();
	
	$("#jobsearch")
		.autocomplete(jobList)
		.result(function(event, item) {
			  if (currentGraph) {
				  currentGraph.selectAndCenter(item);
			  }
			});
	
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
			//window.location.href = contextURL + "/";
			if (data.success) {
				$("#modelDialog").text(data.message);
				$("#modelDialog").dialog(
					{
						width: '300px',
						modal: true,
						close: function(event, ui) {
							window.location.href = contextURL + "/flow?id=" + data.id;
						}
					}
				);
			}
			else if (data.error) {
				$("#modelDialog").text(data.message);
				$("#modelDialog").dialog(
					{
						width: '300px',
						modal: true,
						close: function(event, ui) {
							if (data.id) {
								window.location.href = contextURL + "/flow?id=" + data.id;
							}
							else {
								window.location.href = contextURL + "/flow?job_id=" + name;
							}
						}
					}
				);
			}
		}
	});

}

function showHelpScreen() {
	$("#nonModelDialog").dialog(
		{
			width: '600px',
			dialogClass: 'helpdialog'
		}
	);
}

function resizeSVG(item) {
	svgElement = document.getElementById("graph");
	alert(item.getHeight() + ","+ item.getWidth())
}

window.onload = function() {
	if (window.addEventListener) document.getElementById("content2").addEventListener('DOMMouseScroll', zoomGraph, false);
	document.getElementById("content2").onmousewheel = zoomGraph;
}
