package azkaban.monitor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.app.AzkabanApplication;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.FlowManager;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeGlobalStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;

public class MonitorIntegrationTest {

    private File testScratchPlace;
    private File jobsDir;
    private File logDir;
    private File tempDir;
    private AzkabanApplication application;
    private MonitorImpl monitor;

    private File makeScratchDir(String name) throws IOException {
        File dir = new File(testScratchPlace, "jobs");
        FileUtils.forceMkdir(dir);
        return dir;
    }

    @Before
    public void setUp() throws IOException {
        testScratchPlace = File.createTempFile("scratch", "", new File("dist"));
        FileUtils.forceDelete(testScratchPlace);
        FileUtils.forceMkdir(testScratchPlace);
        System.out.println("Using test scratch place of: "
                + testScratchPlace.getAbsolutePath());

        this.jobsDir = makeScratchDir("jobs");
        this.logDir = makeScratchDir("logs");
        this.tempDir = makeScratchDir("tmp");
        FileUtils.writeLines(new File(jobsDir, "hw.job"), Arrays.asList(
                "type=command", "command=echo hello world"));
        MonitorImpl.unsetMonitor();
        

        application = new AzkabanApplication(Arrays
                .asList(jobsDir), logDir, tempDir, false);
        monitor = MonitorImpl.getMonitor();

    }

    @Test(timeout=10000)
    public void testSingleJob() throws IOException, InterruptedException {
        // The goal of this test is not to check correctness of the book-
        // keeping, but rather to make sure that the book-keeping happens at
        // all.

        final CountDownLatch countDown = new CountDownLatch(1);
        monitor.registerWorkflowClassNotification(new MonitorListener() {
            
            @Override
            public void onWorkflowNotify(NativeWorkflowClassStats wfStats) {
                System.out.println("In onWorkflowNotify!");
                if (wfStats.getNumTimesWorkflowSuccessful() > 0) {
                    countDown.countDown();
                }
            }
            
            @Override
            public void onJobNotify(NativeJobClassStats jobStats) { }
            
            @Override
            public void onGlobalNotify(GlobalNotificationType type,
                    ClassStats statsObject) { }
        }, "hw");
        final FlowManager flowManager = application.getAllFlows();
        ExecutableFlow flowToRun = flowManager.createNewExecutableFlow("hw");
        application.getJobExecutorManager().execute(flowToRun);

        countDown.await();
        // check a bunch of statistics
        NativeGlobalStats globalStats = monitor.getGlobalAzkabanStats();
        Assert.assertEquals(1, globalStats.getHighFlowId());
        Assert.assertEquals(1, globalStats.getTotalWorkflowsStarted());
        Assert.assertEquals(1, globalStats.getTotalWorkflowsSuccessful());
        Assert.assertEquals(1, globalStats.getTotalJobsStarted());
        Assert.assertEquals(1, globalStats.getTotalJobsSuccessful());

        Assert.assertEquals("The number of workflows is incorrect.", 1, monitor
                .getNumberOfWorkflows());
    }

    @Test
    public void testScheduledJob() {
        DateTime scheduleTime = new DateTime().plusDays(1);
        application.getScheduleManager().schedule("hw", scheduleTime, false);
        
        Assert.assertEquals(1, monitor.getGlobalAzkabanStats().getTotalWorkflowsScheduled());
    }
}
