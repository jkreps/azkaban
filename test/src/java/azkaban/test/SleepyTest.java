package azkaban.test;

/**
 * A Job that just sleeps for the given amount of time
 * 
 * @author jkreps
 * 
 */
public class SleepyTest {

    private final long _sleepTimeMs;

    public SleepyTest() {
        this._sleepTimeMs = 5000;
    }

    public void run() throws Throwable {
        System.out.println("Sleeping for " + _sleepTimeMs);
        Thread.sleep(_sleepTimeMs);

        System.out.println("Finished Sleeping");
    }

}
