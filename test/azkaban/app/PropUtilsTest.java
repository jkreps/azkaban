package azkaban.app;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import azkaban.common.utils.Props;

public class PropUtilsTest {
    
    @Before
    public void setUp() throws Exception
    {
    }
    
    @After
    public void tearDown() throws Exception
    {
    }
      
    @Test
    public void testResolveProps() throws Exception {
        Props inputProps = new Props();
        Props genProps = new Props();
        Props outputProps;
        
        inputProps.put("firstName", "Francesco");
        inputProps.put("fillIn", "the guilty party is ${firstName} ${lastName}");
        genProps.put("lastName", "Estaban");
        
        outputProps = PropsUtils.resolveProps(inputProps, genProps);       
        String answer = outputProps.get("fillIn");
        System.out.println(answer);
        
        System.out.println("PropsUtilTest.testResolveProps: " + answer);
        
        Assert.assertEquals("the guilty party is Francesco Estaban", answer);
    }
}
