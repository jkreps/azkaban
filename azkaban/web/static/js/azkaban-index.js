

function getList(data) {
	var jobName = data.name;
	
	var li = document.createElement("li");
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

function expandFlow(folderDiv) {
	var folderId = folderDiv.id;
	
	if (!folderDiv['fold']) {
		var foldableDiv = document.getElementById(folderId + "-panel");
		$(foldableDiv).hide();
		folderDiv['fold'] = foldableDiv;
		foldableDiv['hidden'] = true;
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
			}
		});
	}
	
	var foldable = folderDiv['fold'];
	if (foldable['hidden']) {
		$(foldable).show('medium');
		foldable['hidden'] = false;
	}
	else {
		$(foldable).hide('medium');
		foldable['hidden'] = true;
	}
}