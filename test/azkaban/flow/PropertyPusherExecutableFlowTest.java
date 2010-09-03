package azkaban.flow;

import azkaban.common.utils.Props;
import org.easymock.Capture;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class PropertyPusherExecutableFlowTest
{
    ExecutableFlow propertyFlow;
    ExecutableFlow childFlow;
    FlowCallback callback;

    PropertyPusherExecutableFlow flow;

    @Before
    public void setUp()
    {
        propertyFlow = EasyMock.createMock(ExecutableFlow.class);
        childFlow = EasyMock.createMock(ExecutableFlow.class);
        callback = EasyMock.createMock(FlowCallback.class);

        EasyMock.expect(childFlow.getStatus()).andReturn(Status.READY).times(3);
        EasyMock.expect(childFlow.getStartTime()).andReturn(null).once();
        EasyMock.expect(childFlow.getName()).andReturn("Something").once();

        EasyMock.expect(propertyFlow.getStatus()).andReturn(Status.READY).once();

        EasyMock.replay(childFlow, propertyFlow);

        flow = new PropertyPusherExecutableFlow("blah", "test", propertyFlow, childFlow);

        EasyMock.verify(childFlow, propertyFlow);
        EasyMock.reset(childFlow, propertyFlow);
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public void testSanity() throws Throwable
    {
        Props props = new Props();
        props.put("billy", "blank");

        Capture<Props> propsCap = new Capture<Props>();
        Capture<FlowCallback> propertyFlowCallbackCap = new Capture<FlowCallback>();
        Capture<FlowCallback> actualFlowCallbackCap = new Capture<FlowCallback>();

        Props otherProps = new Props();
        otherProps.put("sally", "jesse");
        otherProps.put("billy", "bob");  // should be overriden

        propertyFlow.execute(EasyMock.eq(otherProps), EasyMock.capture(propertyFlowCallbackCap));
        EasyMock.expectLastCall();

        EasyMock.replay(propertyFlow);

        flow.execute(otherProps, callback);

        EasyMock.verify(propertyFlow);
        EasyMock.reset(propertyFlow);

        EasyMock.expect(propertyFlow.getReturnProps()).andReturn(props).once();

        callback.progressMade();
        EasyMock.expectLastCall();

        childFlow.execute(EasyMock.capture(propsCap), EasyMock.capture(actualFlowCallbackCap));
        EasyMock.expectLastCall();
        EasyMock.replay(childFlow, propertyFlow, callback);

        propertyFlowCallbackCap.getValue().completed(Status.SUCCEEDED);

        EasyMock.verify(childFlow, propertyFlow, callback);
        EasyMock.reset(childFlow, propertyFlow, callback);

        callback.completed(Status.SUCCEEDED);
        EasyMock.expectLastCall();

        final Props someProps = new Props();
        someProps.put("a", "b");

        EasyMock.expect(childFlow.getStatus()).andReturn(Status.SUCCEEDED);
        EasyMock.expect(childFlow.getReturnProps()).andReturn(someProps).once();

        EasyMock.replay(childFlow, callback);

        actualFlowCallbackCap.getValue().completed(Status.SUCCEEDED);

        EasyMock.verify(callback, childFlow);

        Props cappedProps = propsCap.getValue();
        Assert.assertEquals(2, cappedProps.size());
        Assert.assertEquals("blank", cappedProps.get("billy"));
        Assert.assertEquals("jesse", cappedProps.get("sally"));
        Assert.assertEquals(someProps.size(), flow.getReturnProps().size());
        Assert.assertEquals(someProps.get("a"), flow.getReturnProps().get("a"));

    }

}
