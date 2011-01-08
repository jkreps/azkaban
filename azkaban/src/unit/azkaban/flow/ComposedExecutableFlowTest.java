package azkaban.flow;

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

/**
 *
 */
public class ComposedExecutableFlowTest
{
    private volatile Props props;

    private volatile ExecutableFlow dependerFlow;
    private volatile ExecutableFlow dependeeFlow;
    private ComposedExecutableFlow flow;
    private static Map<String,Throwable> theExceptions;
    private static Map<String, Throwable> emptyExceptions;
    
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
        dependerFlow = EasyMock.createStrictMock(ExecutableFlow.class);
        dependeeFlow = EasyMock.createStrictMock(ExecutableFlow.class);

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.replay(dependerFlow, dependeeFlow, props);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);
    }

    @After
    public void tearDown() throws Exception
    {
        EasyMock.verify(dependerFlow);
        EasyMock.verify(dependeeFlow);
        EasyMock.verify(props);
    }

    @Test
    public void testSanity() throws Exception
    {
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.eq(props), EasyMock.capture(dependeeCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee already ran!?", dependeeRan.compareAndSet(false, true));
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependeeCallback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        final Props intermediateReturnProps = new Props();
        intermediateReturnProps.put("some","value1--");
        intermediateReturnProps.put("other", "value2--");
        EasyMock.expect(dependeeFlow.getReturnProps()).andReturn(intermediateReturnProps).times(2);
        
        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        final Capture<Props> intermediatePropsCap = new Capture<Props>();
        dependerFlow.execute(EasyMock.capture(intermediatePropsCap), EasyMock.capture(dependerCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee must run before depender", dependeeRan.get());
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependerCallback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();

        EasyMock.expect(dependerFlow.getExceptions()).andReturn(emptyExceptions).times(1);
        
        EasyMock.reset(props);
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow, props);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                    }
                });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        final Props intProps = intermediatePropsCap.getValue().local();
        Assert.assertTrue(intProps != null);
        Assert.assertEquals(2, intProps.size());
        Assert.assertEquals("value1--", intProps.get("some"));
        Assert.assertEquals("value2--", intProps.get("other"));

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                    }
                });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        EasyMock.verify(props, dependerFlow);
        EasyMock.reset(props, dependerFlow);

        final Props someProps = new Props();
        someProps.put("some", "yay");
        someProps.put("something", "you");
        EasyMock.expect(dependerFlow.getReturnProps()).andReturn(someProps).times(1);
        EasyMock.expect(props.equalsProps(props)).andReturn(false).once();

        EasyMock.replay(props, dependerFlow);

        final Props retProps = flow.getReturnProps();
        final Props expectedProps = new Props();
        expectedProps.put("some", "yay");
        expectedProps.put("something", "you");
        expectedProps.put("other", "value2--");

        Assert.assertTrue(
                String.format("Return props should be the combination of all sub-flow return props[%s].  Was[%s]", expectedProps, retProps),
                retProps.equalsProps(expectedProps)
        );

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
    public void testFailureInDependee() throws Exception
    {
      
         final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.eq(props), EasyMock.capture(dependeeCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee already ran!?", dependeeRan.compareAndSet(false, true));
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependeeCallback.getValue().completed(Status.FAILED);

                return null;
            }
        }).once();
        EasyMock.expect(dependeeFlow.getExceptions()).andReturn(theExceptions).once();

        EasyMock.reset(props);
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow, props);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testFailureInDepender() throws Exception
    {
      
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.eq(props), EasyMock.capture(dependeeCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee already ran!?", dependeeRan.compareAndSet(false, true));
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependeeCallback.getValue().completed(Status.SUCCEEDED);

                return null;
            }
        }).once();
        EasyMock.expect(dependeeFlow.getReturnProps()).andReturn(new Props());

        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.isA(Props.class), EasyMock.capture(dependerCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee must run before depender", dependeeRan.get());
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependerCallback.getValue().completed(Status.FAILED);

                return null;
            }
        }).once();
        EasyMock.expect(dependerFlow.getExceptions()).andReturn(theExceptions).once();

        EasyMock.reset(props);
        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow, props);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.FAILED, status);
                    }
                });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theExceptions, flow.getExceptions());

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testAllExecutesHaveTheirCallbackCalled() throws Exception
    {
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);
        final AtomicBoolean executeCallWhileStateWasRunningHadItsCallbackCalled = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.eq(props), EasyMock.capture(dependeeCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee already ran!?", dependeeRan.compareAndSet(false, true));
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                flow.execute(
                        props,
                        new OneCallFlowCallback(executeCallWhileStateWasRunningHadItsCallbackCalled)
                        {
                            @Override
                            protected void theCallback(Status status)
                            {
                            }
                        });

                dependeeCallback.getValue().completed(Status.SUCCEEDED);

                Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());

                return null;
            }
        }).once();
        EasyMock.expect(dependeeFlow.getReturnProps()).andReturn(new Props()).once();

        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.isA(Props.class), EasyMock.capture(dependerCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee must run before depender", dependeeRan.get());
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                dependerCallback.getValue().completed(Status.SUCCEEDED);

                Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());

                return null;
            }
        }).once();

        EasyMock.reset(props);
        EasyMock.expect(dependerFlow.getExceptions()).andReturn(emptyExceptions).times(1);

        EasyMock.expect(props.equalsProps(props)).andReturn(true).times(2);

        EasyMock.replay(dependerFlow, dependeeFlow, props);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                    }
                });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertTrue("dependeeFlow, upon completion, sends another execute() call to the flow.  " +
                          "The callback from that execute call was apparently not called.",
                          executeCallWhileStateWasRunningHadItsCallbackCalled.get());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        callbackRan = new AtomicBoolean(false);
        flow.execute(
                props,
                new OneCallFlowCallback(callbackRan) {
                    @Override
                    protected void theCallback(Status status)
                    {
                        Assert.assertEquals(Status.SUCCEEDED, status);
                    }
                });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testChildren() throws Exception
    {
        EasyMock.replay(dependeeFlow, dependerFlow);

        Assert.assertTrue("ComposedExecutableFlow should have children.", flow.hasChildren());
        Assert.assertEquals(1, flow.getChildren().size());
        Assert.assertEquals(dependeeFlow, flow.getChildren().get(0));
    }

    @Test
    public void testInitializationDependeeFailed() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);
        final DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).once();
        EasyMock.expect(dependeeFlow.getEndTime()).andReturn(expectedEndTime);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationDependerFailed() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);
        final DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);
        EasyMock.expect(dependerFlow.getEndTime()).andReturn(expectedEndTime);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationBothFailed() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);
        final DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);
        EasyMock.expect(dependerFlow.getEndTime()).andReturn(expectedEndTime);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationDependeeSucceeded() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(1);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
        Assert.assertEquals(props, flow.getParentProps());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationDependeeRunning() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(1);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props).times(1);

        Capture<FlowCallback> dependeeFlowCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.eq(props), EasyMock.capture(dependeeFlowCallback));
        EasyMock.expect(dependeeFlow.getReturnProps()).andReturn(new Props()).once();

        Capture<FlowCallback> dependerFlowCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.isA(Props.class), EasyMock.capture(dependerFlowCallback));

//<<<<<<< HEAD:azkaban/src/unit/azkaban/flow/ComposedExecutableFlowTest.java
        EasyMock.expect(dependerFlow.getExceptions()).andReturn(emptyExceptions).times(1);
        
        EasyMock.replay(dependerFlow, dependeeFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        DateTime beforeTheEnd = new DateTime();
        dependeeFlowCallback.getValue().completed(Status.SUCCEEDED);
        dependerFlowCallback.getValue().completed(Status.SUCCEEDED);

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
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testInitializationDependerRunning() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);
        EasyMock.expect(dependeeFlow.getParentProps()).andReturn(props).once();
        EasyMock.expect(dependeeFlow.getReturnProps()).andReturn(new Props()).once();

        Capture<FlowCallback> dependerFlowCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.isA(Props.class), EasyMock.capture(dependerFlowCallback));

        EasyMock.expect(dependerFlow.getExceptions()).andReturn(emptyExceptions).times(1);
        
        EasyMock.replay(dependerFlow, dependeeFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());

        DateTime beforeTheEnd = new DateTime();
        dependerFlowCallback.getValue().completed(Status.SUCCEEDED);

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

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testCancel() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(true).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected cancel to be successful", flow.cancel());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testUnsuccessfulCancel1() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(false).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testUnsuccessfulCancel2() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(true).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(false).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

    @Test
    public void testUnsuccessfulCancel3() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(false).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(false).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(emptyExceptions, flow.getExceptions());
    }

}
