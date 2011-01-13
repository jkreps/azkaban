function JobControler() {

  function init() {
	  $(".sched-tree").treeview({
	    url: "test.php",  
        collapsed: true,
        animated: "medium",
        persist: "cookie"
      });
    
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
    
  }
  
  // public methods
  return {
    init: init
  }
}

var job_controller = new JobControler();

// add on-dom-ready event to trigger init function
$(document).ready(job_controller.init);
