

function getList(data) {
	var jobName = data.name;
	
	var li = document.createElement("li");
	li['jobname'] = jobName;
	// Setup checkbox
	var input = document.createElement("input"); 
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
	flowButton.setAttribute("id", jobName + "-button");
	flowButton.setAttribute("class", "flowViewButton");
	flowButton.setAttribute("href",contextURL + "/flow?job_id=" + jobName);
	$(flowButton).addClass("hidden");
	li.appendChild(flowButton);
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
	var flowButton = jobname + "-button";
	if (show) {
		$("#" + flowButton).removeClass("hidden");
		$("#" + flowButton).addClass("show");
	}
	else {
		$("#" + flowButton).removeClass("show");
		$("#" + flowButton).addClass("hidden");
	}
}

function expandFlow(folderDiv) {
	var folderId = folderDiv.id;
	
	if (!folderDiv['fold']) {
		$(folderDiv).removeClass('expand');
		$(folderDiv).addClass('wait');
		
		var foldableDiv = document.getElementById(folderId + "-panel");
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
				var ul = document.createElement("ul");
				ul.setAttribute("class", "sched-tree");
				
				for(var i = 0; i < data.length; i++) {
					var root = getList(data[i]);
					ul.appendChild(root);
				}
				
				foldableDiv.appendChild(ul);
				$(ul).treeview({ 
			        collapsed: true,
			        animated: "medium"
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
			$(foldable).show('medium');
			$(folderDiv).removeClass('expand');
			$(folderDiv).addClass('collapse');
			foldable['hidden'] = false;
		}
		else {
			$(foldable).hide('medium');
			$(folderDiv).removeClass('collapse');
			$(folderDiv).addClass('expand');
			foldable['hidden'] = true;
		}
	}
}