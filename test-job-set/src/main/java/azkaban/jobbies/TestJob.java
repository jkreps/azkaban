package azkaban.jobbies;

import azkaban.common.utils.Props;

/**
 *
 */
public class TestJob
{
    private final String id;
    private final Props props;

    public TestJob(String id, Props props)
    {
        this.id = id;
        this.props = props;
    }

    public void run()
    {
        for (String key : props.getKeySet()) {
            System.out.printf("key[%s] -> value[%s]%n", key, props.get(key));
        }

        if (props.containsKey("fail")) {
            throw new RuntimeException("Fail!");
        }
    }

    public Props getJobGeneratedProperties()
    {
        return new Props(null, props.getMapByPrefix("pass-on."));
    }
}
