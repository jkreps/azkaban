(function($) {
	var createRow = function(data) {
		var tr = document.createElement("tr");
		var td = document.createElement("td");
		var a = document.createElement("a");
		$(a).text(data.name);
		
		td.appendChild(a);
		tr.appendChild(td);
		return tr;
	}
	
	var createPanel = function(id) {
		var childRow = document.getElementById(id + "-child");
		if (childRow.innerChildren) {
			var td = document.createElement("td");
			
			$(td).attr("colspan", headerRows);
			$(td).addClass("innerTd");
			var table = settings.dataTablefunc.apply(this);

			for (var i = 0; i < childRow.innerChildren.length; ++i) {
				childRow.innerChildren[i].absstart = childRow.starttime;
				childRow.innerChildren[i].absend = childRow.endtime;
				var tr = settings.dataRowfunc.apply(this, [childRow.innerChildren[i]]);
				table.appendChild(tr);
			}
			td.appendChild(table);

			childRow.appendChild(td);
			childRow.innerChildren = null;
		}
	}
	
	var headerRows = 1;

	var settings = {
    		data : [{name: 'test', children: [{name: 'test2'},{name: 'test3'}]}],
    		headerRowfunc: function(rowData) {
    			return createRow.apply(this, [rowData]);
    		},
    		dataTablefunc : function() {
    			var table = document.createElement("table");
    			return table;
    		},
    		dataRowfunc : function(rowData) {
    			return createRow.apply(this, [rowData]);
    		},
    		style : "azkaban-exec",
    		lastExpanded : "-1"
    	};
	
	var methods = {		
			init : function(options) {
		    	if (options) {
		    		$.extend( settings, options );
		    	}
				return this.each(function() {
					var $this = $(this);
					$this.settings = settings;

					for (var i = 0; i < settings.data.length; ++i) {
						$(this).addClass(settings.style);
						var headerTr = settings.headerRowfunc.apply(this, [settings.data[i]]);
						
						var childTr = document.createElement("tr");
						childTr.innerChildren = settings.data[i].children;
						childTr.starttime = settings.data[i].starttime;
						childTr.endtime = settings.data[i].endtime;
						$(childTr).attr("id", settings.data[i].id + "-child");
						$(childTr).addClass("childRow");
						headerRows = $(headerTr).children().length;
						
						this.appendChild(headerTr);
						this.appendChild(childTr);
						$(childTr).hide();
						
						if (settings.data[i].id == settings.lastExpanded) {
							createPanel(settings.data[i].id);
							var childRow = document.getElementById(settings.data[i].id + "-child");
							$(childRow).show();
						}
					}
				});
			},
			show : function(id, time) {
				if (time == null) {
					time = 0;
				}

				createPanel(id);
				var childRow = document.getElementById(id + "-child");
				$(childRow).fadeIn(time);
			},
			hide : function(id, time) {
				if (time == null) {
					time = 0;
				}

				var childRow = document.getElementById(id + "-child");
				$(childRow).fadeOut(time);
			}
		};

	$.fn.azExecTable = function(method) {
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