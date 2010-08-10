package azkaban.utils.process;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import azkaban.util.process.AzkabanProcess;
import azkaban.util.process.AzkabanProcessBuilder;
import azkaban.util.process.ProcessFailureException;

import static org.junit.Assert.*;

public class ProcessTest {
    
  
    @Test
    public void helloWorld() throws Exception { 
        AzkabanProcess process = new AzkabanProcessBuilder("echo", "hello", "world").build();
        assertTrue("Process is not started.", !process.isStarted());
        assertTrue("Process is not running.", !process.isRunning());
        process.run();
        assertTrue("Process should be set.", process.getProcessId() > 0);
        assertTrue("After running, process should be complete.", process.isComplete());
        assertTrue("Process is not running.", !process.isRunning());
    }
    
    @Test(expected = ProcessFailureException.class)
    public void testFailOnNonZeroExitCode() throws Exception {
        new AzkabanProcessBuilder("ls", "alkdjfalsjdflkasdjf").build().run();
    }
    
    @Test(expected = IOException.class)
    public void testFailOnBadCommand() throws Exception {
        new AzkabanProcessBuilder("alkdjfalsjdflkasdjf").build().run();
    }
    
      
    @Test
    public void testKill() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        AzkabanProcess p1 = new AzkabanProcessBuilder("sleep", "10").build();
        runInSeperateThread(executor, p1);
        assertTrue("Soft kill should interrupt sleep.", p1.softKill(5, TimeUnit.SECONDS));
        p1.awaitCompletion();
        
        AzkabanProcess p2 = new AzkabanProcessBuilder("sleep", "10").build();
        runInSeperateThread(executor, p2);
        p2.hardKill();
        p2.awaitCompletion();
        assertTrue(p2.isComplete());
    }
    
    private Future<Object> runInSeperateThread(final ExecutorService executor, final AzkabanProcess process) throws InterruptedException {
        Future<Object> result = executor.submit(new Callable<Object>() {
            public Object call() throws IOException {
                process.run();
                return null;
            }
        });
        process.awaitStartup();
        return result;
    }
    
}
