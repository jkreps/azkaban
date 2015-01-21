function prepareChildData(childTr, data) {
	childTr.starttime = data.starttime;
	childTr.endtime = data.endtime;
}

function prepareRowResults(childRow) {
	if (childRow.innerChildren) {
		var td = document.createElement("td");

		var starttime = currentTime;
		var endtime = 1;
		var isRunning = false;
		
		for (var i = 0; i < childRow.innerChildren.length; ++i) {
			var innerChildRow = childRow.innerChildren[i];
			if (innerChildRow.starttime) {
				starttime = Math.min(starttime, innerChildRow.starttime);
			}
			if (innerChildRow.endtime) {
				endtime = Math.max(endtime, innerChildRow.endtime);
			}
			
			if (innerChildRow.status == "running" ) {
				isRunning = true;
			}
		}
		
		if (isRunning) {
			endtime = currentTime;
		}
		
		for (var i = 0; i < childRow.innerChildren.length; ++i) {
			childRow.innerChildren[i].absstart = starttime;
			childRow.innerChildren[i].absend = endtime;
		}
	}
}

function createGantzChart(status, absStart, absEnd, start, end) {
	var groupDiv = document.createElement("div");
	$(groupDiv).addClass("gantts");
	if (absStart == null || start == null) {
		$(groupDiv).text("N/A");
		$(groupDiv).attr("title", "Status: " + status);
		$(groupDiv).tooltip({
			position: "center right",
			offset: [0, 10],
			effect: "fade",
			opacity: 0.9,
			predelay: 100
		});
		return groupDiv;
	}
	
	if (absEnd == null) {
		absEnd = currentTime;
	}
	
	if (end == null) {
		end = currentTime;
	}
	
	var total = absEnd - absStart;
	var startPercent = 0;
	var percentWidth = 100;
	if (total > 0) {
		startPercent = 100*(start - absStart)/total;
		percentWidth = 100*(end - start)/total;
		if (percentWidth > 100) {
			percentWidth = 100;
		}
		else if(percentWidth < 1) {
			percentWidth = 1;
		}
		
		if (startPercent + percentWidth > 98) {
			if (percentWidth == 1) {
				startPercent = 97;
			}
			else {
				percentWidth = 98 - startPercent;
			}
		}
	}
	
	var generalBar = document.createElement("div");
	$(generalBar).css('margin-left', startPercent + "%");
	$(generalBar).css('width', percentWidth + "%");
	$(generalBar).addClass(status);
	
	groupDiv.appendChild(generalBar);
	$(groupDiv).attr("title", "Status: " + status);
	$(groupDiv).tooltip({
		position: "center right",
		offset: [0, 10],
		effect: "fade",
		opacity: 0.9,
		predelay: 100
	});
	return groupDiv;
}

function getDate(date) {
	if (date) {
		return date;
	}
	else {
		return "-";
	}
}

function presetToggle(id) {
	
	var hit = document.getElementById("hitdiv-" + id);
	if (hit == null) {
		return;
	}
	
	if (hit.expand) {
		$("#execTable").azExecTable("hide", id, 0);
		$(hit).removeClass("collapse");
		hit.expand = false;
	}
	else {
		$("#execTable").azExecTable("show", id, 0);
		$(hit).addClass("collapse");
		hit.expand = true;
	}
}

function toggleExpand(id, speed) {
	if (speed == null) {
		speed = 'fast';
	}
	
	var hit = document.getElementById("hitdiv-" + id);
	if (hit.expand) {
		$("#execTable").azExecTable("hide", id, speed);
		$(hit).removeClass("collapse");
		hit.expand = false;
		$.cookie(persistVar, "-1");
	}
	else {
		$("#execTable").azExecTable("show", id, speed);
		$(hit).addClass("collapse");
		hit.expand = true;
		$.cookie(persistVar, id);
	}
}

function getDuration(data) {
	var startMs = data.starttime;
	var endMs = data.endtime;
	if (startMs) {
		if (endMs == null) {
			if (data.status == "running") {
				endMs = currentTime;
			}
			else {
				return "-";
			}
		}
		
		var diff = endMs - startMs;
		var seconds = Math.floor(diff / 1000);
		
		if (seconds < 60) {
			return seconds + " sec";
		}
		
		var mins = Math.floor(seconds / 60);
		seconds = seconds % 60;
		if (mins < 60) {
			return mins + "m " + seconds + "s";
		}

		var hours = Math.floor(mins / 60);
		mins = mins % 60;
		if (hours < 24) {
			return hours + "h " + mins + " m" + seconds + "s";
		}
		
		var days = Math.floor(hours / 24);
		hours = hours % 24;
		
		return days + "d " + hours + "h " + mins + "m " + seconds + "s";
	}

	return "-";
}

var headerRowfunc = function(data) {
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
	$(hitDiv).attr("id", "hitdiv-" + data.id);
	hitDiv.expand = false;
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
	
	var statusTd = document.createElement("td");
	$(statusTd).text(data.status);
	
	var actionTd = document.createElement("td");
	var actionA = document.createElement("a");
	$(actionA).text("view/restart");
	$(actionA).attr("href", contextURL + "/flow?action=restart&id=" + data.id);
	actionTd.appendChild(actionA);
	
	//Add to row
	tr.appendChild(idTD);
	tr.appendChild(titleTd);
	tr.appendChild(startedTd);
	tr.appendChild(endedTd);
	tr.appendChild(durrationTd);
	tr.appendChild(statusTd);
	tr.appendChild(actionTd);
	return tr;
}

var dataTablefunc = function() {
	var table = document.createElement("table");
	$(table).addClass("innerTable");
	
	// Create Colgroup
	var colgroup = document.createElement("colgroup");
	var nameCol = document.createElement("col");
	var timelineCol = document.createElement("col");
	var startedCol = document.createElement("col");
	var endedCol = document.createElement("col");
	var durationCol = document.createElement("col");
	var statusCol = document.createElement("col");
	$(nameCol).addClass("titlecol");
	$(timelineCol).addClass("timeline");
	$(startedCol).addClass("datetime");
	$(endedCol).addClass("datetime");
	$(durationCol).addClass("duration");
	$(statusCol).addClass("status");
	colgroup.appendChild(nameCol);
	colgroup.appendChild(timelineCol);
	colgroup.appendChild(startedCol);
	colgroup.appendChild(endedCol);
	colgroup.appendChild(durationCol);
	colgroup.appendChild(statusCol);
	table.appendChild(colgroup);
	
	var tr = document.createElement("tr");
	
	var nameTh = document.createElement("th");
	$(nameTh).text("Name");
	var timeLineTh = document.createElement("th");
	$(timeLineTh).text("Timeline");
	var startedTh = document.createElement("th");
	$(startedTh).text("Started (" + timezone + ")");
	var endedTh = document.createElement("th");
	$(endedTh).text("Ended (" + timezone + ")");
	var durationTh = document.createElement("th");
	$(durationTh).text("Duration");
	var statusTh = document.createElement("th");
	$(statusTh).text("Status");
	
	tr.appendChild(nameTh);
	tr.appendChild(timeLineTh);
	tr.appendChild(startedTh);
	tr.appendChild(endedTh);
	tr.appendChild(durationTh);
	tr.appendChild(statusTh);
	table.appendChild(tr);
	
	return table;
}

var dataRowfunc = function(data) {
	var tr = document.createElement("tr");
	var nameTd = document.createElement("td");
	$(nameTd).addClass("titlecol");
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
	nameTd.appendChild(a);
	
	var timelineTd = document.createElement("td");
	var innerTimeline = createGantzChart(data.status, data.absstart, data.absend, data.starttime, data.endtime);
	timelineTd.appendChild(innerTimeline);
	
	var startedTd = document.createElement("td");
	$(startedTd).text(getDate(data.starttimestr));
	
	var endedTd = document.createElement("td");
	$(endedTd).text(getDate(data.endtimestr));
	
	var durationTd = document.createElement("td");
	$(durationTd).text(getDuration(data));
	
	var statusTd = document.createElement("td");
	$(statusTd).text(data.status);
	
	tr.appendChild(nameTd);
	tr.appendChild(timelineTd);
	tr.appendChild(startedTd);
	tr.appendChild(endedTd);
	tr.appendChild(durationTd);
	tr.appendChild(statusTd);
	
	return tr;
}

function getLastPersisted() {
	var persisted = $.cookie(persistVar);
	if (persisted == null) {
		return "-1";
	}
	
	return persisted;
}