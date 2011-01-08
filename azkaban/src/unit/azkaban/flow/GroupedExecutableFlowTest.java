package azkaban.flow;

import azkaban.app.JobManager;
import azkaban.common.utils.Props;
import azkaban.jobs.Status;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class GroupedExecutableFlowTest
{
    private volatile Props props;
    private volatile ExecutableFlow mockFlow1;
    private volatile ExecutableFlow mockFlow2;

    private volatile GroupedExecutableFlow flow;

   private static Map<String,Throwable> theExceptions;
   private static Map<String,Throwable> emptyExceptions;
   
    
    @BeforeClass
    public static void init() throws Exception {
      theExceptions = new HashMap<String,Throwable>();
      theExceptions.put("main", new RuntimeException());
      
      emptyExceptions = new HashMap<String, Throwable>();
    }
    
    @Before
    public void setUp() throws Exception
    {
        props = EasyMock.createStrictMock(Props.class);
        mockFlow1 = EasyMock.createMock(ExecutableFlow.class);
        mockFlow2 = EasyMock.createMock(ExecutableFlow.class);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();
        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);
        Assert.assertEquals("1 + 2", flow.getName());

        EasyMock.verify(mockFlow1, mockFlow2, props);
        EasyMock.reset(mockFlow1, mockFlow2, props);
    }

    @After
    public void tearDown() throws Exception
    {
        EasyMock.verify(mockFlow1);
        EasyMock.verify(mockFlow2);
        EasyMock.verify(props);
    }

    @Test
    public void testSanity() throws Exception
    {
        final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(flow1Callback));
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

        Props mockFlow1Props = new Props();
        mockFlow1Props.put("1", "1");
        mockFlow1Props.put("2", "1");

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(mockFlow1Props).once();

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(flow2Callback));
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

        Props mockFlow2Props = new Props();
        mockFlow2Props.put("2", "2");
        mockFlow2Props.put("3", "2");
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(mockFlow2Props).once();

        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    public void theCallback(Status status) {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                        Assert.assertEquals(2, numJobsComplete.get());
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan)
                {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                        Assert.assertEquals(2, numJobsComplete.get());
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        Props retProps = flow.getReturnProps();
        Assert.assertEquals(3, retProps.size());
        Assert.assertEquals("1", retProps.get("1"));
        Assert.assertEquals("2", retProps.get("2"));
        Assert.assertEquals("2", retProps.get("3"));

        EasyMock.verify(props);
        EasyMock.reset(props);

        EasyMock.expect(props.equalsProps(props)).andReturn(false).once();

        EasyMock.replay(props);

        boolean exceptionThrown = false;
        try {
            flow.execute(
                    props,
                    new FlowCallback() {
                        @Override
                        public void progressMade() {
                        }

                        @Override
                        public void completed(Status status) {
                        }
                    }
            );
        }
        catch (IllegalArgumentException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue("Expected an IllegalArgumentException to be thrown because props weren't the same.", exceptionThrown);
    }

    @Test
    public void testFailureJob1() throws Exception
    {
    
      final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(flow1Callback));
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
        EasyMock.expect(mockFlow1.getExceptions()).andReturn(theExceptions).times(1);

        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        /**** Setup mockFlow2 ****/
        EasyMock.expect(mockFlow2.getExceptions()).andReturn(emptyExceptions).times(1);
        EasyMock.replay(mockFlow1, mockFlow2, props);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    public void theCallback(Status status) {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan)
                {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testFailureJob2() throws Exception
    {
    
      final AtomicLong numJobsComplete = new AtomicLong(0);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(flow1Callback));
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
        EasyMock.expect(mockFlow1.getExceptions()).andReturn(emptyExceptions).times(1);

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(flow2Callback));
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

      final RuntimeException e1 = new RuntimeException();
      final RuntimeException e2 = new RuntimeException();
      
      final Map<String, Throwable> e1s = new HashMap<String, Throwable>();
      e1s.put("e1", e1);
      e1s.put("e2", e2);

      EasyMock.expect(mockFlow2.getExceptions()).andReturn(e1s).times(1);
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    public void theCallback(Status status) {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(e1s, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan)
                {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                        Assert.assertEquals(2, numJobsComplete.get());
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(e1s, flow.getExceptions());

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testAllCallbacksCalled() throws Exception
    {
        final AtomicLong numJobsComplete = new AtomicLong(0);
        final AtomicBoolean executeCallWhileStateWasRunningHadItsCallbackCalled = new AtomicBoolean(false);

        /**** Setup mockFlow1 ****/
        final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(flow1Callback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                Assert.assertEquals(Status.RUNNING, flow.getStatus());
                Assert.assertEquals(1, numJobsComplete.incrementAndGet());

                flow.execute(
                        props,
                        new OneCallFlowCallback(executeCallWhileStateWasRunningHadItsCallbackCalled)
                        {
                            @Override
                            protected void theCallback(Status status)
                            {
                            }
                        });

                flow1Callback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        /**** Setup mockFlow2 ****/
        final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(flow2Callback));
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

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();
        
        EasyMock.expect(props.equalsProps(props)).andReturn(true).times(2);

        EasyMock.replay(mockFlow1, mockFlow2, props);

        /**** Start the test ****/
        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    public void theCallback(Status status) {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                        Assert.assertEquals(2, numJobsComplete.get());
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertTrue("mockFlow1, upon completion, sends another execute() call to the flow.  " +
                          "The callback from that execute call was apparently not called.",
                          executeCallWhileStateWasRunningHadItsCallbackCalled.get());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan)
                {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                        Assert.assertEquals(2, numJobsComplete.get());
                    }
                });

        Assert.assertTrue("Callback wasn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testChildren() throws Exception
    {
        EasyMock.replay(mockFlow1, mockFlow2, props);

        Assert.assertTrue("GroupedExecutableFlow should have children.", flow.hasChildren());
        Assert.assertEquals(2, flow.getChildren().size());
        Assert.assertEquals(mockFlow1, flow.getChildren().get(0));
        Assert.assertEquals(mockFlow2, flow.getChildren().get(1));
    }

    @Test
    public void testAllBaseJobsCompleted() throws Exception
    {
        EasyMock.replay(mockFlow1, mockFlow2, props);

        final JobManager factory = EasyMock.createStrictMock(JobManager.class);
        EasyMock.replay(factory);

        final IndividualJobExecutableFlow completedJob1 = new IndividualJobExecutableFlow("blah", "blah", factory);
        final IndividualJobExecutableFlow completedJob2 = new IndividualJobExecutableFlow("blah", "blah", factory);

        flow = new GroupedExecutableFlow(
                "blah",
                completedJob1,
                completedJob2
        );

        completedJob1.markCompleted();
        completedJob2.markCompleted();

        AtomicBoolean callbackWasCalled = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackWasCalled)
                {
                    @Override
                    public void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                    }
                });

        Assert.assertTrue("Callback wasn't called!?", callbackWasCalled.get());
        EasyMock.verify(factory);
    }

    @Test
    public void testInitializationFirstSucceeded() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
    }

    @Test
    public void testInitializationSecondSucceeded() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
//<<<<<<< HEAD:azkaban/src/unit/azkaban/flow/GroupedExecutableFlowTest.java
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(falseEndTime).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(falseEndTime).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(1);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
    }

    @Test
    public void testInitializationFirstFailedSecondReady() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(null).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
    }

    @Test
    public void testInitializationFirstFailedSecondRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);
        DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.FAILED).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.RUNNING).once();

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(expectedEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(null).once();

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
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

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(2);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.FAILED).times(2);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow1.getEndTime()).andReturn(falseEndTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getEndTime()).andReturn(expectedEndTime).once();

        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());
    }

    @Test
    public void testInitializationFirstRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(null).once();

        Capture<FlowCallback> callbackCapture = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(callbackCapture));

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(mockFlow1, mockFlow2);
        EasyMock.reset(mockFlow1, mockFlow2);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.READY).once();

        EasyMock.replay(mockFlow1, mockFlow2);

        callbackCapture.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationSecondRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.RUNNING).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(null).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        Capture<FlowCallback> callbackCapture = new Capture<FlowCallback>();
        mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(callbackCapture));

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(mockFlow1, mockFlow2, props);
        EasyMock.reset(mockFlow1, mockFlow2, props);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        callbackCapture.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationBothRunning() throws Exception
    {
        DateTime expectedStartTime = new DateTime(0);
        DateTime falseStartTime = new DateTime(1);

        EasyMock.expect(mockFlow1.getName()).andReturn("a").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("b").once();

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).times(3);
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.RUNNING).times(3);

        EasyMock.expect(mockFlow1.getStartTime()).andReturn(falseStartTime).once();
        EasyMock.expect(mockFlow2.getStartTime()).andReturn(expectedStartTime).once();

        Capture<FlowCallback> callbackCapture1 = new Capture<FlowCallback>();
        Capture<FlowCallback> callbackCapture2 = new Capture<FlowCallback>();
        mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(callbackCapture1));
        mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(callbackCapture2));

        EasyMock.expect(mockFlow1.getParentProps()).andReturn(props).once();
        EasyMock.expect(mockFlow2.getParentProps()).andReturn(props).once();
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.expect(mockFlow1.getName()).andReturn("1").once();
        EasyMock.expect(mockFlow2.getName()).andReturn("2").once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        flow = new GroupedExecutableFlow("blah", mockFlow1, mockFlow2);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());

        EasyMock.verify(mockFlow1, mockFlow2, props);
        EasyMock.reset(mockFlow1, mockFlow2, props);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

        Assert.assertSame(callbackCapture1.getValue(), callbackCapture2.getValue());

        callbackCapture1.getValue().completed(Status.SUCCEEDED);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(mockFlow1, mockFlow2, props);
        EasyMock.reset(mockFlow1, mockFlow2, props);

        EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(mockFlow2.getStatus()).andReturn(Status.SUCCEEDED).once();

        EasyMock.expect(mockFlow1.getReturnProps()).andReturn(new Props()).once();
        EasyMock.expect(mockFlow2.getReturnProps()).andReturn(new Props()).once();

        EasyMock.replay(mockFlow1, mockFlow2, props);

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
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }
    
    
    
    @Test
    public void testBothFailured() throws Exception
    {
    
      final RuntimeException e1 = new RuntimeException();
      final RuntimeException e2 = new RuntimeException();
      
      final Map<String, Throwable> e1s = new HashMap<String, Throwable>();
      e1s.put("e1", e1);
      
      final Map<String, Throwable> e2s = new HashMap<String, Throwable>();
      e2s.put("e2", e2);
      
      final Map<String, Throwable> expected = new HashMap<String, Throwable>();
      expected.putAll(e1s);
      expected.putAll(e2s);
      
      final AtomicLong numJobsComplete = new AtomicLong(0);

      /**** Setup mockFlow1 ****/
      final Capture<FlowCallback> flow1Callback = new Capture<FlowCallback>();
      
      mockFlow1.execute(EasyMock.eq(props), EasyMock.capture(flow1Callback));
      EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
          @Override
          public Void answer() throws Throwable {
              Assert.assertEquals(Status.RUNNING, flow.getStatus());
              Assert.assertEquals(1, numJobsComplete.incrementAndGet());

              flow1Callback.getValue().completed(Status.FAILED);

              return null;
          }
      }).once();

      EasyMock.expect(mockFlow1.getStatus()).andReturn(Status.SUCCEEDED).times(2);
      EasyMock.expect(mockFlow1.getExceptions()).andReturn(e1s).times(1);

      /**** Setup mockFlow2 ****/
      final Capture<FlowCallback> flow2Callback = new Capture<FlowCallback>();
      
      mockFlow2.execute(EasyMock.eq(props), EasyMock.capture(flow2Callback));
      
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

    EasyMock.expect(mockFlow2.getExceptions()).andReturn(e2s).times(1);

    EasyMock.expect(props.equalsProps(props)).andReturn(true).once();
    
    EasyMock.replay(mockFlow1, mockFlow2, props);

      /**** Start the test ****/
      AtomicBoolean callbackRan = new AtomicBoolean(false);
      flow.execute(props, new OneCallFlowCallback(callbackRan) {
          @Override
          public void theCallback(Status status) {
              Assert.assertEquals(Status.FAILED, status);
          }
      });

      Assert.assertTrue("Callback wasn't run.", callbackRan.get());
      Assert.assertEquals(Status.FAILED, flow.getStatus());

      callbackRan = new AtomicBoolean(false);
      flow.execute(props, new OneCallFlowCallback(callbackRan)
      {
          @Override
          protected void theCallback(Status status)
          {
              Assert.assertEquals(Status.FAILED, status);
              Assert.assertEquals(2, numJobsComplete.get());
          }
      });

      Assert.assertTrue("Callback wasn't run.", callbackRan.get());
      Assert.assertEquals(Status.FAILED, flow.getStatus());
      Assert.assertEquals(expected, flow.getExceptions());

      Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
      Assert.assertEquals(Status.READY, flow.getStatus());
      Assert.assertEquals(emptyExceptions, flow.getExceptions());

    
    }

}
