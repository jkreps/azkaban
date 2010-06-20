package azkaban.utils.process;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import azkaban.util.process.AzkabanProcess;
import azkaban.util.process.AzkabanProcessBuilder;
import azkaban.util.process.ProcessFailureException;

import static org.junit.Assert.*;

public class ProcessTest {
    
    @Test
    public void helloWorld() { 
        AzkabanProcess process = new AzkabanProcessBuilder("echo", "hello", "world").build();
        assertTrue("Process is not started.", !process.isStarted());
        assertTrue("Process is not running.", !process.isRunning());
        process.run();
        assertTrue("Process should be set.", process.getProcessId() > 0);
        assertTrue("After running, process should be complete.", process.isComplete());
        assertTrue("Process is not running.", !process.isRunning());
    }
    
    @Test(expected = ProcessFailureException.class)
    public void testFailOnNonZeroExitCode() {
        new AzkabanProcessBuilder("ls", "alkdjfalsjdflkasdjf").build().run();
    }
    
    @Test(expected = ProcessFailureException.class)
    public void testFailOnBadCommand() {
        new AzkabanProcessBuilder("alkdjfalsjdflkasdjf").build().run();
    }
    
    @Test
    public void testKill() throws InterruptedException {
        Executor executor = Executors.newFixedThreadPool(2);
        
        final AzkabanProcess p1 = new AzkabanProcessBuilder("sleep", "10").build();
        executor.execute(new Runnable() {
            public void run() {
                p1.run();
            }
        });
        p1.awaitStartup();
        assertTrue("Soft kill should interrupt sleep.", p1.softKill(5, TimeUnit.SECONDS));
        
        final AzkabanProcess p2 = new AzkabanProcessBuilder("sleep", "10").build();
        executor.execute(new Runnable() {
            public void run() {
                p2.run();
            }
        });
        p2.awaitStartup();
        p2.hardKill();
        Thread.sleep(1000);
        assertTrue(p2.isComplete());
    }
    
    @Test
    public void testEnv() throws InterruptedException {

    }
    
}
