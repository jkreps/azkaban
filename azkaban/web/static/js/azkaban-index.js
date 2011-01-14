$(function () {
    var treeElems = $(".execing-jobs");
    if (treeElems.length > 0)
    {
      var execingJobsTree;
      for (i=0; i<treeElems.length; i++)
      {
        $("#"+treeElems[i].id).treeview({
        collapsed: true,
        animated: "medium",
        persist: "cookie"
      });    
      }
    }
    
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

function expandFlow(folderDiv) {
	var folderId = folderDiv.id;
	var d = new Date();
	if (!folderDiv['fold']) {
		$.cookie(folderId, d.getTime());
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