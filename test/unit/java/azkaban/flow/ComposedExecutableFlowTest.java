package azkaban.flow;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ComposedExecutableFlowTest
{
    private volatile ExecutableFlow dependerFlow;
    private volatile ExecutableFlow dependeeFlow;
    private ComposedExecutableFlow flow;

    @Before
    public void setUp() throws Exception
    {
        dependerFlow = EasyMock.createStrictMock(ExecutableFlow.class);
        dependeeFlow = EasyMock.createStrictMock(ExecutableFlow.class);

        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.replay(dependerFlow, dependeeFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);
    }

    @After
    public void tearDown() throws Exception
    {
        EasyMock.verify(dependerFlow);
        EasyMock.verify(dependeeFlow);
    }

    @Test
    public void testSanity() throws Exception
    {
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.capture(dependeeCallback));
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

        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.capture(dependerCallback));
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

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
            }
        });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
            }
        });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testFailureInDependee() throws Exception
    {
        final RuntimeException theException = new RuntimeException();
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.capture(dependeeCallback));
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
        EasyMock.expect(dependeeFlow.getException()).andReturn(theException).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
            }
        });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theException, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
            }
        });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theException, flow.getException());

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testFailureInDepender() throws Exception
    {
        RuntimeException theException = new RuntimeException();
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.capture(dependeeCallback));
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

        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.capture(dependerCallback));
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
        EasyMock.expect(dependerFlow.getException()).andReturn(theException).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
            }
        });

        Assert.assertTrue("Internal flow executes never ran.", dependeeRan.get());
        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theException, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.FAILED, status);
            }
        });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(theException, flow.getException());

        EasyMock.verify(dependerFlow, dependeeFlow);
        EasyMock.reset(dependerFlow, dependeeFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected to be able to reset the flow", flow.reset());
        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testAllExecutesHaveTheirCallbackCalled() throws Exception
    {
        final AtomicBoolean dependeeRan = new AtomicBoolean(false);
        final AtomicBoolean executeCallWhileStateWasRunningHadItsCallbackCalled = new AtomicBoolean(false);

        final Capture<FlowCallback> dependeeCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.capture(dependeeCallback));
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
        {
            @Override
            public Object answer() throws Throwable
            {
                Assert.assertTrue("Dependee already ran!?", dependeeRan.compareAndSet(false, true));
                Assert.assertEquals(Status.RUNNING, flow.getStatus());

                flow.execute(new OneCallFlowCallback(executeCallWhileStateWasRunningHadItsCallbackCalled)
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

        final Capture<FlowCallback> dependerCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.capture(dependerCallback));
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

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());

        AtomicBoolean callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
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
        Assert.assertEquals(null, flow.getException());

        callbackRan = new AtomicBoolean(false);
        flow.execute(new OneCallFlowCallback(callbackRan) {
            @Override
            protected void theCallback(Status status)
            {
                Assert.assertEquals(Status.SUCCEEDED, status);
            }
        });

        Assert.assertTrue("Callback didn't run.", callbackRan.get());
        Assert.assertEquals(Status.SUCCEEDED, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
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
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationDependerFailed() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);
        final DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);
        EasyMock.expect(dependerFlow.getEndTime()).andReturn(expectedEndTime);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationBothFailed() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);
        final DateTime expectedEndTime = new DateTime(100);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.FAILED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);
        EasyMock.expect(dependerFlow.getEndTime()).andReturn(expectedEndTime);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.FAILED, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(expectedEndTime, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationDependeeSucceeded() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.SUCCEEDED).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(1);
        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationDependeeRunning() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.READY).once();
        EasyMock.expect(dependeeFlow.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(1);

        Capture<FlowCallback> dependeeFlowCallback = new Capture<FlowCallback>();
        dependeeFlow.execute(EasyMock.capture(dependeeFlowCallback));

        Capture<FlowCallback> dependerFlowCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.capture(dependerFlowCallback));

        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

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
        Assert.assertEquals(null, flow.getException());
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testInitializationDependerRunning() throws Exception
    {
        final DateTime expectedStartTime = new DateTime(0);

        EasyMock.expect(dependerFlow.getStatus()).andReturn(Status.RUNNING).once();
        EasyMock.expect(dependeeFlow.getStartTime()).andReturn(expectedStartTime).times(2);

        Capture<FlowCallback> dependerFlowCallback = new Capture<FlowCallback>();
        dependerFlow.execute(EasyMock.capture(dependerFlowCallback));

        EasyMock.replay(dependeeFlow, dependerFlow);

        flow = new ComposedExecutableFlow("blah", dependerFlow, dependeeFlow);

        Assert.assertEquals(Status.RUNNING, flow.getStatus());
        Assert.assertEquals(expectedStartTime, flow.getStartTime());
        Assert.assertEquals(null, flow.getEndTime());
        Assert.assertEquals(null, flow.getException());

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
        Assert.assertEquals(null, flow.getException());

        EasyMock.verify(dependerFlow);
        EasyMock.reset(dependerFlow);

        EasyMock.expect(dependerFlow.reset()).andReturn(true);
        EasyMock.replay(dependerFlow);

        flow.reset();

        Assert.assertEquals(Status.READY, flow.getStatus());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testCancel() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(true).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertTrue("Expected cancel to be successful", flow.cancel());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testUnsuccessfulCancel1() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(false).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(true).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testUnsuccessfulCancel2() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(true).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(false).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(null, flow.getException());
    }

    @Test
    public void testUnsuccessfulCancel3() throws Exception
    {
        EasyMock.expect(dependerFlow.cancel()).andReturn(false).once();
        EasyMock.expect(dependeeFlow.cancel()).andReturn(false).once();

        EasyMock.replay(dependerFlow, dependeeFlow);

        Assert.assertFalse("Expected cancel to be UN-successful", flow.cancel());
        Assert.assertEquals(null, flow.getException());
    }
}
