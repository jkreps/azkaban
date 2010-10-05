package azkaban.app;

import azkaban.common.jobs.AbstractJob;

/**
 * this job is used to throw out exception caught in initialization stage
 * 
 * @author lguo
 *
 */
public class InitErrorJob extends AbstractJob
{

  private Exception exception;
  
  public InitErrorJob (String id, Exception e) {
     super(id);
     exception = e;
  }
  
  @Override
  public void run() throws Exception
  {
    throw exception;
  }

}
