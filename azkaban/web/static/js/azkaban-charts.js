(function($) {
	var uniqueCounter = 1;
	
	var fakeHide = function(children, time) {
		if (children) {
			for (var i = 0; i < children.length; ++i) {
				$(children[i]).fadeOut(time);
				if (children[i].isExpanded != null) {
					fakeHide(children[i].childTR, time);
				}
			}
		}
	}
	
	var fakeShow = function(children, time) {
		if (children) {
			for (var i = 0; i < children.length; ++i) {
				$(children[i]).fadeIn(time);
				if (children[i].isExpanded) {
					fakeShow(children[i].childTR, time);
				}
			}
		}
	}

	var expandFunc = function(hitDiv, treecolumnfunc, datacolumnfunc ) {
		var tr = document.getElementById(hitDiv.rowID);
		
		if (hitDiv.expanded && tr.uniqueCounter) {
			fakeHide(tr.childTR, 10);
			hitDiv.expanded = false;
			$(hitDiv).removeClass("collapse");
			tr.isExpanded = false;
		}
		else {
			if (tr.unprocessedChildren) {
				var children = groupFunc(treecolumnfunc, datacolumnfunc, tr.unprocessedChildren, tr.level + 1);
				for (var i = children.length -1; i >= 0; --i) {
					$(children[i]).hide();
					$(children[i]).addClass(hitDiv.rowID + "-child");
					$(tr).after(children[i]);
				}
				
				tr.childTR = children;
				
				tr.unprocessedChildren = null;
			}
			
			tr.isExpanded = true;
			$(hitDiv).addClass("collapse");
			fakeShow(tr.childTR, 500);
			hitDiv.expanded = true;
		}
	}
	
	var rowfunc = function(rowData, treecolumnfunc, datacolumnfunc, level, isLast) {
		var tr = document.createElement("tr");
		var td = document.createElement("td");
		tr.appendChild(td);
		tr.unprocessedChildren = rowData.children;
		tr.level = level;
		tr.uniqueCounter = uniqueCounter++;
		tr.id = rowData.name + uniqueCounter;
		tr.name = rowData.name;

		$(td).css("padding-left", (20 * level) + "px");
		$(td).addClass("treetable-treecol");
		
		var treeLines = document.createElement("div");
		$(treeLines).addClass("treeline");
		
		if (tr.unprocessedChildren) {
			var hitDiv = document.createElement("div");
			hitDiv.expanded = false;
			$(hitDiv).addClass("hitarea");
			$(hitDiv).addClass("expandable-hitarea");
			treeLines.appendChild(hitDiv);
			hitDiv.rowID = tr.id;
			
			$(hitDiv).click(function() {
				expandFunc.apply(this,[hitDiv, treecolumnfunc, datacolumnfunc ]);
			});
		}
		
		if (isLast) {
			$(treeLines).addClass("last");
		}
		
		td.appendChild(treeLines);
		
		var treecolval = treecolumnfunc.apply(this, [rowData]);
		td.appendChild(treecolval);

		return tr;
	}

	var groupFunc = function( treecolumnfunc, datacolumnfunc, data, level) {
		var rows = new Array();
		for (var i = 0; i < data.length; ++i) {
			var tr = rowfunc(data[i], treecolumnfunc, datacolumnfunc, level, i == data.length - 1);

			var columns = datacolumnfunc(data[i], 0);
			for (var j = 0; j < columns.length; ++j) {
				if (columns[j].tagName == "td") {
					tr.appendChild(columns[j]);
				}
				else {
	    			var td = document.createElement("td");
	    			td.appendChild(columns[j]);
	    			tr.appendChild(td);
				}
			}
			
			rows[i] = tr;
		}
		
		return rows;
	}
	
	var methods = {		
		init : function(options) {
	    	var settings = {
	    		data : [{name: 'test', children: [{name: 'test2'},{name: 'test3'}]}],
	    		treecolumnfunc: function(rowData) {
	    			var a = document.createElement("a");
	    			$(a).text(rowData.name);
	    			return a;
	    		},
	    		datacolumnfunc : function(rowData) {
	    			var arr = new Array();
	    			return arr;
	    		}
	    	};
	    	if (options) {
	    		$.extend( settings, options );
	    	}
			return this.each(function() {
				var $this = $(this);
				$(this).addClass("azkaban-charts");

				var rows = groupFunc(settings.treecolumnfunc, settings.datacolumnfunc, settings.data, 0);
				for (var i = 0; i < rows.length; ++i) {
					this.appendChild(rows[i]);
				}
			});
		}
	};
	
	$.fn.azTreeTable = function(method) {
		if (methods[method]) {
			return methods[method].apply(this,Array.prototype.slice.call( arguments, 1 ));
		}
		else if ( typeof method === 'object' || ! method ) {
	      return methods.init.apply( this, arguments );
	    } else {
	      $.error( 'Method ' +  method + ' does not exist on jQuery.tooltip' );
	    }  
	}
	
})(jQuery);