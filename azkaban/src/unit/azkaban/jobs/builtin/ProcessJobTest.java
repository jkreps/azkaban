package azkaban.jobs.builtin;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.easymock.classextension.EasyMock;

import azkaban.app.JobDescriptor;
import azkaban.common.utils.Props;
import azkaban.jobs.AbstractProcessJob;
import azkaban.jobs.builtin.ProcessJob;


public class ProcessJobTest
{
  private ProcessJob job = null;
  private JobDescriptor descriptor = null;
  private Props props = null;
  @Before
  public void setUp() {
    
    /*  initialize job */
    descriptor = EasyMock.createMock(JobDescriptor.class);
    
    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, ".");
    
    EasyMock.expect(descriptor.getId()).andReturn("process").times(1);
    EasyMock.expect(descriptor.getResolvedProps()).andReturn(props).times(1);
    EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);
    
    EasyMock.replay(descriptor);
    
    job = new ProcessJob(descriptor);
    
    EasyMock.verify(descriptor);
  }
  
  @Test
  public void testOneUnixCommand() {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "ls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    job.run();
    
  }

  @Test
  public void testFailedUnixCommand() {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "xls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    try {
      job.run();
    }catch (RuntimeException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }
    
    @Test
    public void testMultipleUnixCommands( ) {
      /* initialize the Props */
      props.put(ProcessJob.WORKING_DIR, ".");
      props.put(ProcessJob.COMMAND, "pwd");
      props.put("command.1", "date");
      props.put("command.2", "whoami");
      
      job.run();
    }
}


