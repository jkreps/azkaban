function runJob(id, withDep, contextURL, callback, modal) {
	if (!callback) {
		callback = function(){};
	}
	jQuery.ajax( {
		'type': 'POST',
		'url': contextURL + "/call", 
		'async':false,
		'data': { 
			  "action":"run_job",
			  "id":id,
			  "include_deps" : withDep
			},
		'success': function(data) {
				var title = "Info";
				
				if (data.success) {
					$("#modelDialog").text(data.success);
				}
				else {
					title = "ERROR";
					$("#modelDialog").text(data.error);
				}
				
				// Just take in account the null/undefined case.
				var isModal = false;
				if (modal) {
					isModal = true;
				}
				
				$("#modelDialog").dialog(
					{
						width: '350px',
						modal: isModal,
						close: function(event, ui) {
							callback();
						},
						title: title,
						buttons : [
				            {
				                text: "Ok",
				                click: function() { $(this).dialog("close"); }
				            }]
					}
				);
		}
	}
	);
}