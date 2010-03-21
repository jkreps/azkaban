package azkaban.flow;

import azkaban.app.JobFactory;
import azkaban.app.JobDescriptor;
import azkaban.common.jobs.Job;
import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class IndividualJobExecutableFlowTest
{
    private volatile JobFactory jobFactory;
    private volatile JobDescriptor jobDescriptor;

    private volatile AtomicBoolean assertionViolated;
    private volatile String reason;

    @Before
    public void setUp()
    {
        jobFactory = EasyMock.createMock(JobFactory.class);
        jobDescriptor = EasyMock.createMock(JobDescriptor.class);

        assertionViolated = new AtomicBoolean(false);
        reason = "Default Reason";
    }

    @After
    public void tearDown()
    {
        Assert.assertFalse(reason, assertionViolated.get());
        EasyMock.verify(jobFactory, jobDescriptor);
    }

    @Test
    public void testSanity() throws Throwable
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);

        final Job mockJob = EasyMock.createMock(Job.class);
        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow("blah", jobFactory, jobDescriptor);

        EasyMock.expect(jobFactory.apply(jobDescriptor)).andReturn(mockJob).once();
        EasyMock.expect(mockJob.getId()).andReturn("success Job").once();

        mockJob.run();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>()
        {
            @Override
            public Void answer() throws Throwable
            {
                Assert.assertEquals(Status.RUNNING, executableFlow.getStatus());

                return null;
            }
        }).once();

        EasyMock.replay(mockJob, jobFactory, jobDescriptor);

        Assert.assertEquals(Status.READY, executableFlow.getStatus());

        executableFlow.execute(new FlowCallback()
        {
            @Override
            public void progressMade()
            {
                assertionViolated.set(true);
                reason = String.format("progressMade() shouldn't actually be called.");
            }

            @Override
            public void completed(Status status)
            {
                completionLatch.countDown();
                if (Status.SUCCEEDED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback: status[%s] != Status.SUCCEEDED", status);
                }
            }
        });

        completionLatch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.SUCCEEDED, executableFlow.getStatus());

        EasyMock.verify(mockJob);

        Assert.assertTrue("Expected to be able to reset the executableFlow.", executableFlow.reset());
        Assert.assertEquals(Status.READY, executableFlow.getStatus());

    }

    @Test
    public void testFailure() throws Throwable
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);

        final Job mockJob = EasyMock.createMock(Job.class);
        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow("blah", jobFactory, jobDescriptor);

        EasyMock.expect(jobFactory.apply(jobDescriptor)).andReturn(mockJob).once();
        EasyMock.expect(mockJob.getId()).andReturn("failure Job").once();

        mockJob.run();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>()
        {
            @Override
            public Void answer() throws Throwable
            {
                Assert.assertEquals(Status.RUNNING, executableFlow.getStatus());

                throw new RuntimeException("Fail!");
            }
        }).once();

        EasyMock.replay(mockJob, jobFactory, jobDescriptor);

        Assert.assertEquals(Status.READY, executableFlow.getStatus());

        executableFlow.execute(new FlowCallback()
        {
            @Override
            public void progressMade()
            {
                assertionViolated.set(true);
                reason = String.format("progressMade() shouldn't actually be called.");
            }

            @Override
            public void completed(Status status)
            {
                completionLatch.countDown();
                if (Status.FAILED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback: status[%s] != Status.FAILED", status);
                }
            }
        });

        completionLatch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.FAILED, executableFlow.getStatus());

        EasyMock.verify(mockJob);

        Assert.assertTrue("Expected to be able to reset the executableFlow.", executableFlow.reset());
        Assert.assertEquals(Status.READY, executableFlow.getStatus());
    }

    @Test
    public void testNoChildren() throws Exception
    {
        EasyMock.replay(jobFactory, jobDescriptor);
        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow("blah", jobFactory, jobDescriptor);

        Assert.assertFalse("IndividualJobExecutableFlow objects should not have any children.", executableFlow.hasChildren());
        Assert.assertTrue("IndividualJobExecutableFlow objects should not return any children.", executableFlow.getChildren().isEmpty());
    }

    @Test
    public void testAllExecuteCallbacksCalledOnSuccess() throws Throwable
    {
        final CountDownLatch firstCallbackLatch = new CountDownLatch(1);
        final CountDownLatch secondCallbackLatch = new CountDownLatch(1);

        final Job mockJob = EasyMock.createMock(Job.class);
        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow("blah", jobFactory, jobDescriptor);

        EasyMock.expect(jobFactory.apply(jobDescriptor)).andReturn(mockJob).once();
        EasyMock.expect(mockJob.getId()).andReturn("success Job").once();

        mockJob.run();
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>()
        {
            @Override
            public Void answer() throws Throwable
            {
                Assert.assertEquals(Status.RUNNING, executableFlow.getStatus());

                return null;
            }
        }).once();

        EasyMock.replay(mockJob, jobFactory, jobDescriptor);

        Assert.assertEquals(Status.READY, executableFlow.getStatus());

        final AtomicBoolean firstCallbackCalled = new AtomicBoolean(false);
        executableFlow.execute(new OneCallFlowCallback(firstCallbackCalled)
        {
            @Override
            public void theCallback(Status status)
            {
                firstCallbackLatch.countDown();
                if (Status.SUCCEEDED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback1: status[%s] != Status.SUCCEEDED", status);
                }
            }
        });

        final AtomicBoolean secondCallbackCalled = new AtomicBoolean(false);
        executableFlow.execute(new OneCallFlowCallback(secondCallbackCalled)
        {
            @Override
            public void theCallback(Status status)
            {
                secondCallbackLatch.countDown();
                if (Status.SUCCEEDED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback2: status[%s] != Status.SUCCEEDED", status);
                }
            }
        });

        firstCallbackLatch.await(1000, TimeUnit.MILLISECONDS);
        secondCallbackLatch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.SUCCEEDED, executableFlow.getStatus());

        EasyMock.verify(mockJob);

        Assert.assertTrue("First callback wasn't called?", firstCallbackCalled.get());
        Assert.assertTrue("Second callback wasn't called?", secondCallbackCalled.get());
        Assert.assertTrue("Expected to be able to reset the executableFlow.", executableFlow.reset());
        Assert.assertEquals(Status.READY, executableFlow.getStatus());

    }

    @Test
    public void testAllExecuteCallbacksCalledOnFailure() throws Throwable
    {
        final CountDownLatch firstCallbackLatch = new CountDownLatch(1);
        final CountDownLatch secondCallbackLatch = new CountDownLatch(1);

        final Job mockJob = EasyMock.createMock(Job.class);
        final IndividualJobExecutableFlow executableFlow = new IndividualJobExecutableFlow("blah", jobFactory, jobDescriptor);

        EasyMock.expect(jobFactory.apply(jobDescriptor)).andReturn(mockJob).once();
        EasyMock.expect(mockJob.getId()).andReturn("success Job").once();

        mockJob.run();
        EasyMock.expectLastCall().andThrow(new RuntimeException()).once();

        EasyMock.replay(mockJob, jobFactory, jobDescriptor);

        Assert.assertEquals(Status.READY, executableFlow.getStatus());

        final AtomicBoolean firstCallbackCalled = new AtomicBoolean(false);
        executableFlow.execute(new OneCallFlowCallback(firstCallbackCalled)
        {
            @Override
            public void theCallback(Status status)
            {
                firstCallbackLatch.countDown();
                if (Status.FAILED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback1: status[%s] != Status.SUCCEEDED", status);
                }
            }
        });

        final AtomicBoolean secondCallbackCalled = new AtomicBoolean(false);
        executableFlow.execute(new OneCallFlowCallback(secondCallbackCalled)
        {
            @Override
            public void theCallback(Status status)
            {
                secondCallbackLatch.countDown();
                if (Status.FAILED != status) {
                    assertionViolated.set(true);
                    reason = String.format("In executableFlow Callback2: status[%s] != Status.SUCCEEDED", status);
                }
            }
        });

        firstCallbackLatch.await(1000, TimeUnit.MILLISECONDS);
        secondCallbackLatch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(Status.FAILED, executableFlow.getStatus());

        EasyMock.verify(mockJob);

        Assert.assertTrue("First callback wasn't called?", firstCallbackCalled.get());
        Assert.assertTrue("Second callback wasn't called?", secondCallbackCalled.get());
        Assert.assertTrue("Expected to be able to reset the executableFlow.", executableFlow.reset());
        Assert.assertEquals(Status.READY, executableFlow.getStatus());

    }
}