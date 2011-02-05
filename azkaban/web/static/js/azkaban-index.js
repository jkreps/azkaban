var persistVar = "azIndex";

$(function () {
	var persisted = getLastPersisted();

	$("#execTable").azExecTable({
		'data': execution,
		'style': 'tableHistory jobtable translucent',
		'headerRowfunc': indexheaderRowfunc,
		'dataTablefunc': dataTablefunc,
		'dataRowfunc': dataRowfunc,
		'prepareChildData': prepareChildData,
		'prepareRowResults': prepareRowResults,
		'lastExpanded': persisted
	});

	presetToggle(persisted);
	
    $(".jobfolder").each(function(index) {
    	var d = new Date();
    	var numMinuteThreshold = 15;
    	if ($.cookie(this.id)) {
    		if ($.cookie(this.id) > d.getTime() - numMinuteThreshold*60000) {
    			expandFlow(this);
    		}
    		else {
    			$.cookie(this.id, null);
    		}
    	}
    });
});

function getList(data) {
	var jobName = data.name;
	
	var li = document.createElement("li");
	li['jobname'] = jobName;
	// Setup checkbox
	var input = document.createElement("input"); 
	input.setAttribute("id", jobName + "-checkbox");
	input.setAttribute("type", "checkbox");
	input.setAttribute("name", "jobs");
	input.setAttribute("value", jobName);
	input.setAttribute("class", "sched-tree-checkbox");
	li.appendChild(input);
	
	// Setup anchor
	var a = document.createElement("a");
	a.setAttribute("href", contextURL + "/job?id=" + jobName);
	a.setAttribute("class", "job-name");
	a.setAttribute("name", "sched-tree-link");
	$(a).text(jobName);
	li.appendChild(a);
	
	// Setup flow button
	var flowButton = document.createElement("a");
	$(flowButton).text("View Flow");
	flowButton.setAttribute("id", jobName + "-flowbutton");
	flowButton.setAttribute("class", "flowViewButton");
	flowButton.setAttribute("href",contextURL + "/flow?job_id=" + jobName);
	$(flowButton).addClass("hidden");
	li.appendChild(flowButton);
	
	var runButton = document.createElement("a");
	$(runButton).text("Run");
	runButton.setAttribute("id", jobName + "-runbutton");
	runButton.setAttribute("class", "flowViewButton");
	runButton.setAttribute("href","#");
	$(runButton).addClass("hidden");
	$(runButton).click(function() {
		runJob(jobName, false, contextURL, function(){window.location.reload()}, true);
	});
	li.appendChild(runButton);
	
	var runDepButton = document.createElement("a");
	$(runDepButton).text("Run with Dependencies");
	runDepButton.setAttribute("id", jobName + "-rundepbutton");
	runDepButton.setAttribute("class", "flowViewButton");
	runDepButton.setAttribute("href","#");
	$(runDepButton).addClass("hidden");
	$(runDepButton).click(function() {
		runJob(jobName, true, contextURL, function(){window.location.reload()}, true);
	});
	li.appendChild(runDepButton);
	
	li.setAttribute("onMouseOver", "flowButtonShow(true, this.jobname)");
	li.setAttribute("onMouseOut", "flowButtonShow(false, this.jobname)");
	
	if (data["dep"]) {
		var ul = document.createElement("ul");
		var children = data["dep"];
		for (var i = 0; i < children.length; i++) {
			var childLI = getList(children[i]);
			ul.appendChild(childLI);
		}
		
		li.appendChild(ul);
	}
	
	return li;
}

function flowButtonShow(show, jobname) {
	var flowButton = jobname + "-flowbutton";
	var runButton = jobname + "-runbutton"
	var runDepButton = jobname + "-rundepbutton"
	if (show) {
		$("#" + flowButton).removeClass("hidden");
		$("#" + flowButton).addClass("show");
		$("#" + runButton).removeClass("hidden");
		$("#" + runButton).addClass("show");
		$("#" + runDepButton).removeClass("hidden");
		$("#" + runDepButton).addClass("show");
	}
	else {
		$("#" + flowButton).removeClass("show");
		$("#" + flowButton).addClass("hidden");
		$("#" + runButton).removeClass("show");
		$("#" + runButton).addClass("hidden");
		$("#" + runDepButton).removeClass("show");
		$("#" + runDepButton).addClass("hidden");
	}
}

var createTreeRow = function(data) {
	var jobName = data.name;
	
	var div = document.createElement("div");
	div['jobname'] = jobName;
	$(div).addClass("joblist");
	
	// Setup checkbox
	var input = document.createElement("input"); 
	input.setAttribute("id", jobName + "-checkbox");
	input.setAttribute("type", "checkbox");
	input.setAttribute("name", "jobs");
	input.setAttribute("value", jobName);
	input.setAttribute("class", "sched-tree-checkbox");
	div.appendChild(input);
	
	// Setup anchor
	var a = document.createElement("a");
	a.setAttribute("href", contextURL + "/job?id=" + jobName);
	a.setAttribute("class", "job-name");
	a.setAttribute("name", "sched-tree-link");
	$(a).text(jobName);
	div.appendChild(a);
	
	// Setup flow button
	var flowButton = document.createElement("a");
	$(flowButton).text("View Flow");
	flowButton.setAttribute("id", jobName + "-flowbutton");
	flowButton.setAttribute("class", "flowViewButton");
	flowButton.setAttribute("href",contextURL + "/flow?job_id=" + jobName);
	$(flowButton).addClass("hidden");
	div.appendChild(flowButton);
	
	var runButton = document.createElement("a");
	$(runButton).text("Run");
	runButton.setAttribute("id", jobName + "-runbutton");
	runButton.setAttribute("class", "flowViewButton");
	runButton.setAttribute("href","#");
	$(runButton).addClass("hidden");
	$(runButton).click(function() {
		runJob(jobName, false, contextURL, function(){window.location.reload()}, true);
	});
	div.appendChild(runButton);
	
	var runDepButton = document.createElement("a");
	$(runDepButton).text("Run with Dependencies");
	runDepButton.setAttribute("id", jobName + "-rundepbutton");
	runDepButton.setAttribute("class", "flowViewButton");
	runDepButton.setAttribute("href","#");
	$(runDepButton).addClass("hidden");
	$(runDepButton).click(function() {
		runJob(jobName, true, contextURL, function(){window.location.reload()}, true);
	});
	div.appendChild(runDepButton);
	
	div.setAttribute("onMouseOver", "flowButtonShow(true, this.jobname)");
	div.setAttribute("onMouseOut", "flowButtonShow(false, this.jobname)");
	
	return div;
}

function expandFlow(folderDiv) {
	var folderId = folderDiv.id;
	var d = new Date();
	
	// Unloaded, we load
	if (!folderDiv['fold']) {
		$.cookie(folderId, d.getTime());
		$(folderDiv).removeClass('expand');
		$(folderDiv).addClass('wait');
		
		var foldableDiv = document.getElementById(folderId + "-panel");
		var table = document.getElementById(folderId + "-table");
		$(foldableDiv).hide();
		folderDiv['fold'] = foldableDiv;
		foldableDiv['hidden'] = true;
		$(foldableDiv).hide();
		
		jQuery.ajax({
			"type": "POST",
			"url" : contextURL + "/",
			"data" : {
				"action":"loadjobs",
				"folder": folderId
			},
			success : function(data) {
				
				$(table).azTreeTable({
					"data":data,
					"treecolumnfunc":createTreeRow
				});
				
				foldableDiv['hidden'] = false;
				$(foldableDiv).show('medium');
				$(folderDiv).removeClass('wait');
				$(folderDiv).addClass('collapse');
			}
		});

	}
	else {
		var foldable = folderDiv['fold'];
		if (foldable['hidden']) {
			$.cookie(folderId, d.getTime());
			$(foldable).show('medium');
			$(folderDiv).removeClass('expand');
			$(folderDiv).addClass('collapse');
			foldable['hidden'] = false;
		}
		else {
			$.cookie(folderId, null);
			$(foldable).hide('medium');
			$(folderDiv).removeClass('collapse');
			$(folderDiv).addClass('expand');
			foldable['hidden'] = true;
		}
	}
}

var indexheaderRowfunc = function(data) {
	var tr = document.createElement("tr");
	$(tr).attr("id", data.id);
	
	// ID column
	var idTD = document.createElement("td");
	$(idTD).text(data.id);
	
	// Title column
	var titleTd = document.createElement("td");
	$(titleTd).addClass("titlecol");
	var a = document.createElement("a");
	$(a).attr("href", contextURL + "/job?id=" + data.name + "&logs");
	$(a).attr("title", data.name + "<br>Status: " + data.status);
	$(a).text(data.name);
	$(a).tooltip({
		position: "center right",
		offset: [0, 10],
		effect: "fade",
		opacity: 0.9,
		predelay: 500
	});
	// Hit div
	var hitDiv = document.createElement("div");
	hitDiv.expand = false;
	$(hitDiv).attr("id", "hitdiv-" + data.id);
	$(hitDiv).addClass("arrow");
	$(hitDiv).click(function() {
		toggleExpand(data.id);
	});
	titleTd.appendChild(hitDiv);
	titleTd.appendChild(a);
	
	// Started column
	var startedTd = document.createElement("td");
	$(startedTd).text(getDate(data.starttimestr));
	
	var endedTd = document.createElement("td");
	$(endedTd).text(getDate(data.endtimestr));
	
	var durrationTd = document.createElement("td");
	$(durrationTd).text(getDuration(data));

	
	var form = document.createElement("form");
	$(form).attr("action", contextURL + "/");
	$(form).attr("method", "post");
	$(form).attr("style", "display:inline");
	
	var inputCancel = document.createElement("input");
	$(inputCancel).attr("type", "hidden");
	$(inputCancel).attr("name", "action");
	$(inputCancel).attr("value", "cancel");
	
	var inputJob = document.createElement("input");
	$(inputJob).attr("type", "hidden");
	$(inputJob).attr("name", "job");
	$(inputJob).attr("value", data.id);
	
	var inputSubmit = document.createElement("input");
	$(inputSubmit).attr("type", "submit");
	$(inputSubmit).attr("value", "Cancel");
	
	form.appendChild(inputCancel);
	form.appendChild(inputJob);
	form.appendChild(inputSubmit);
	
	var actionTd = document.createElement("td");
	actionTd.appendChild(form);
	
	//Add to row
	tr.appendChild(idTD);
	tr.appendChild(titleTd);
	tr.appendChild(startedTd);
	tr.appendChild(durrationTd);
	tr.appendChild(actionTd);
	return tr;
}
