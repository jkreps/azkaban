package azkaban.jobbies;

import azkaban.common.utils.Props;

/**
 *
 */
public class FailJob
{
    private final String id;
    private final Props props;

    public FailJob(String id, Props props)
    {
        this.id = id;
        this.props = props;
    }

    public void run()
    {
        for (String key : props.getKeySet()) {
            System.out.printf("key[%s] -> value[%s]%n", key, props.get(key));
        }

        throw new RuntimeException("Fail!");
    }
}
