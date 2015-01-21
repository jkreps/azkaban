package azkaban.flow;

import azkaban.common.utils.Props;
import azkaban.jobs.Status;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
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
public class MultipleDependencyExecutableFlowTest
{
    private volatile Props props;
    private volatile ExecutableFlow dependerFlow;
    private volatile ExecutableFlow dependeeFlow;
    private MultipleDependencyExecutableFlow flow;

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
        dependerFlow = EasyMock.createMock(ExecutableFlow.class);
        dependeeFlow = EasyMock.createMock(ExecutableFlow.class);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).times(1);
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.READY).times(3);

        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(null).once();

        EasyMock.expect(dependeeFlow.getName()).andReturn("1").once();

        EasyMock.replay(dependerFlow, dependeeFlow, props);

        flow = new MultipleDependencyExecutableFlow("blah", dependerFlow, dependeeFlow);

        EasyMock.verify(dependerFlow, dependeeFlow, props);
        EasyMock.reset(dependerFlow, dependeeFlow, props);
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

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.SUCCEEDED).once();
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

                return null;
            }
        }).once();

        EasyMock.expect(dependerFlow.getExceptions()).andReturn(emptyExceptions).times(1);
        
        EasyMock.replay(dependerFlow, dependeeFlow);

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

        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();
        EasyMock.replay(props);

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

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getExceptions()).andReturn(theExceptions).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

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

        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();
        EasyMock.replay(props);
        
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
        Assert.assertEquals(props, flow.getParentProps());

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

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.SUCCEEDED).once();
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

                dependerCallback.getValue().completed(Status.FAILED);

                return null;
            }
        }).once();
        EasyMock.expect(dependerFlow.getExceptions()).andReturn(theExceptions).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

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

        EasyMock.expect(props.equalsProps(props)).andReturn(true).once();
        EasyMock.replay(props);
        
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
        Assert.assertEquals(props, flow.getParentProps());

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

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.SUCCEEDED).once();
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
        EasyMock.replay(dependeeFlow, dependerFlow, props);

        Assert.assertTrue("ComposedExecutableFlow should have children.", flow.hasChildren());
        Assert.assertEquals(1, flow.getChildren().size());
        Assert.assertEquals(dependeeFlow, flow.getChildren().get(0));
    }
}
