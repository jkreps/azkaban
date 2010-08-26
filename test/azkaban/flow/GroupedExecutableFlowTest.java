package azkaban.flow;

import azkaban.app.JobManager;
import azkaban.common.utils.Props;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class GroupedExecutableFlowTest
{
    private volatile ExecutableFlow mockFlow1;
    private volatile ExecutableFlow mockFlow2;

    private volatile GroupedExecutableFlow flow;

    @Before
    public void setUp() throws Exception
    {
        mockFlow1 = EasyMock.createMock(ExecutableFlow.class);
        mockFlow2 = EasyMock.createMock(ExecutableFlow.class);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();
        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);
    }

    @After
    public void tearDown() throws Exception
    {
        EasyMock.verify(mockFlow1);
        EasyMock.verify(mockFlow2);
    }

    @Test
    public void testSanity() throws Exception
    {
        // mockFlow1 generated parameters
        Props mockFlow1GenParams = new Props();
        mockFlow1GenParams.put("junit.mockFlow1", "mockGen1");
        // mockFlow1 generated parameters
        Props mockFlow2GenParams = new Props();
        mockFlow2GenParams.put("junit.mockFlow2", "mockGen2");
        // the group flow's input params
        Props mockGroupFlowInputGenParams = new Props();
        mockGroupFlowInputGenParams.put("junit.groupFlow", "groupFlowGen");
        
        final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(flow1Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(1, numJobsComplete.incrementAndGet());

                flow1Callback.getValue().completed(Status.SUCCEEDED);

                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(mockFlow1GenParams).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockflow1").once();

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.capture(flow2Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(2, numJobsComplete.incrementAndGet());

                flow2Callback.getValue().completed(Status.SUCCEEDED);

                Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow2.getStatus()).andAnswer(new IAnswer<Status>()
        {
            private volatile AtomicInteger count = new AtomicInteger(0);

            @Override
            public Status answer() throws Throwable
            {
                switch (count.getAndIncrement()) {
                    case 0: return Status.READY;
                    case 1: return Status.SUCCEEDED;
                    default: Assert.fail("mockFlow2.getStatus() should only be called 2 times.");
                }
                return null;
            }
        }).times(2);
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(mockFlow2GenParams).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockflow2").once();

        EasyMock.replay(mockFlow1, mockFlow2);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            public void theCallback(Status status) {
                Assert.assertEquals(Status.SUCCEEDED, status);
                Assert.assertEquals(2, numJobsComplete.get());
            }
        }, mockGroupFlowInputGenParams);

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan)
        {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
                Assert.assertEquals(2, numJobsComplete.get());
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
        
        // The output should be the union of the input params to the job _ the generated output params
        // from after the job ran.
        Props flowGeneratedProps = flow.getFlowGeneratedProperties();
        Assert.assertFalse(flowGeneratedProps == null);
        Assert.assertEquals(flowGeneratedProps.get("junit.mockFlow1"), "mockGen1");
        Assert.assertEquals(flowGeneratedProps.get("junit.mockFlow2"), "mockGen2");
        Assert.assertEquals(flowGeneratedProps.get("junit.groupFlow"), "groupFlowGen");
    }

    @Test
    public void testFailureJob1() throws Exception
    {
        final RuntimeException theException = new RuntimeException();
        final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(flow1Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(1, numJobsComplete.incrementAndGet());

                flow1Callback.getValue().completed(Status.FAILED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).times(1);
        EasyMock.expect(mockFlow1.getException()).andReturn(theException).times(1);

        /**** Setup mockFlow2 ****/
        EasyMock.replay(mockFlow1, mockFlow2);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            public void theCallback(Status status) {
                Assert.assertEquals(Status.FAILED, status);
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan)
        {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testFailureJob2() throws Exception
    {
        final RuntimeException theException = new RuntimeException();
        final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(flow1Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(1, numJobsComplete.incrementAndGet());

                flow1Callback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow1.getException()).andReturn(null).times(1);

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.capture(flow2Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(2, numJobsComplete.incrementAndGet());

                flow2Callback.getValue().completed(Status.FAILED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow2.getStatus()).andAnswer(new IAnswer<Status>()
        {
            private volatile AtomicInteger count = new AtomicInteger(0);

            @Override
            public Status answer() throws Throwable
            {
                switch (count.getAndIncrement()) {
                    case 0: return Status.READY;
                    case 1: return Status.FAILED;
                    default: Assert.fail("mockFlow2.getStatus() should only be called 2 times.");
                }
                return null;
            }
        }).times(2);

        EasyMock.expect(mockFlow2.getException()).andReturn(theException).times(1);

        EasyMock.replay(mockFlow1, mockFlow2);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            public void theCallback(Status status) {
                Assert.assertEquals(Status.FAILED, status);
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan)
        {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
                Assert.assertEquals(2, numJobsComplete.get());
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testAllCallbacksCalled() throws Exception
    {
        final AtomicLong numJobsComplete = new AtomicLong(0);
        final AtomicBoolean executeCallWhileStateWasRunningHadItsCallbackCalled = new AtomicBoolean(false);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(flow1Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(1, numJobsComplete.incrementAndGet());

                flow.execute(new OneCallFlowCallback(executeCallWhileStateWasRunningHadItsCallbackCalled)
                {
                    @Override
                    protected void theCallback(Status status)
                    {
                    }
                }, null);

                flow1Callback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").once();

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.capture(flow2Callback), EasyMock.isA(Props.class));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(2, numJobsComplete.incrementAndGet());

                flow2Callback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow2.getStatus()).andAnswer(new IAnswer<Status>()
        {
            private volatile AtomicInteger count = new AtomicInteger(0);

            @Override
            public Status answer() throws Throwable
            {
                switch (count.getAndIncrement()) {
                    case 0: return Status.READY;
                    case 1: return Status.SUCCEEDED;
                    default: Assert.fail("mockFlow2.getStatus() should only be called 2 times.");
                }
                return null;
            }
        }).times(2);
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").once();

        EasyMock.replay(mockFlow1, mockFlow2);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            public void theCallback(Status status) {
                Assert.assertEquals(Status.SUCCEEDED, status);
                Assert.assertEquals(2, numJobsComplete.get());
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertTrue("mockFlow1, upon completion, sends another execute() call to the flow.  " +
                          "The callback from that execute call was apparently not called.",
                          executeCallWhileStateWasRunningHadItsCallbackCalled.get());
        Assert.assertEquals(null, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan)
        {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
                Assert.assertEquals(2, numJobsComplete.get());
            }
        }, new Props());

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testChildren() throws Exception
    {
        EasyMock.replay(mockFlow1, mockFlow2);

        Assert.assertTrue("GroupedExecutableFlow should have children.", flow.hasChildren());
        Assert.assertEquals(2, flow.getChildren().size());
        Assert.assertEquals(mockFlow1, flow.getChildren().get(0));
        Assert.assertEquals(mockFlow2, flow.getChildren().get(1));
    }

    @Test
    public void testAllBaseJobsCompleted() throws Exception
    {
        EasyMock.replay(mockFlow1, mockFlow2);

        final JobManager factory = EasyMock.createStrictMock(JobManager.class);
        EasyMock.replay(factory);

        final IndividualJobExecutableFlow completedJob1 = new IndividualJobExecutableFlow("blah", "blah", new Props(), factory);
        final IndividualJobExecutableFlow completedJob2 = new IndividualJobExecutableFlow("blah", "blah", new Props(), factory);

        flow = new GroupedExecutableFlow(
                "blah",
                completedJob1,
                completedJob2
        );

        completedJob1.markCompleted();
        completedJob2.markCompleted();

        AtomicBoolean callbackWasCalled = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackWasCalled)
        {
            @Override
            public void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
            }
        }, null);

        Assert.assertTrue("Callback wasn't called!?", callbackWasCalled.get());
        EasyMock.verify(factory);
    }

    @Test
    public void testInitializationFirstSucceeded() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationSecondSucceeded() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationBothSucceeded1() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();
        
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").once();
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").once();
                
        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationBothSucceeded2() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(falseEndTime).once();
        
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").once();
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationBothSucceeded3() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();
        
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").once();
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }


    @Test
    public void testInitializationBothSucceeded4() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(falseEndTime).once();
          
        // These are called by mockFlow1.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").once();
        
        // These are called by mockFlow2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationFirstFailed() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationFirstFailedSecondReady() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(null).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationFirstFailedSecondRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(null).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationSecondFailed() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);
        DateTime falseEndTime = new DateTime(99);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.FAILED).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationFirstRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();

        Capture<FlowCallback> callbackCapture = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(callbackCapture), EasyMock.isA(Props.class));

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        callbackCapture.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationSecondRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.RUNNING).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        Capture<FlowCallback> callbackCapture = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.capture(callbackCapture), EasyMock.isA(Props.class));

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        callbackCapture.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationBothRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.RUNNING).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        Capture<FlowCallback> callbackCapture1 = new Capture<FlowCallback>();
        Capture<FlowCallback> callbackCapture2 = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.capture(callbackCapture1), EasyMock.isA(Props.class));
        mockFlow2.execute(EasyMock.capture(callbackCapture2), EasyMock.isA(Props.class));

        EasyMock.replay(mockFlow1, mockFlow2);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        Assert.assertSame(callbackCapture1.getValue(), callbackCapture2.getValue());

        callbackCapture1.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();
        
        // These are called by mockFlow1/2.getValue().completed(Status.SUCCEEDED);
        EasyMock.expect(mockFlow1.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow1.getName()).andReturn("mockFlow1").times(1);
        EasyMock.expect(mockFlow2.getFlowGeneratedProperties()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getName()).andReturn("mockFlow2").times(1);

        EasyMock.replay(mockFlow1, mockFlow2);

        DateTime beforeTheEnd = new DateTime();
        callbackCapture2.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertFalse(
                String.format(
                        "flow's end time[%s] should be after beforeTheEnd[%s]",
                        flow.getEndTime(),
                        beforeTheEnd
                ),
                beforeTheEnd.isAfter(flow.getEndTime())
        );
        Assert.assertEquals(null, flow.getException());
    }
}
