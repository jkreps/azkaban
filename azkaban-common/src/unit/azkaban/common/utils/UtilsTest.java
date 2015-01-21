package azkaban.common.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void TestTail_smallBuffer() {
    try{
        Vector<String> ret = Utils.tail(
                                        "src/unit/azkaban/common/utils/test_tail_file.txt", 
                                        14, 100);
        
        Vector<String> expected = new Vector<String>();
        BufferedReader input = 
            new BufferedReader(new FileReader("src/unit/azkaban/common/utils/test_tail_file_expected.txt"));

        String line = null;
        while ((line = input.readLine()) != null) {
            expected.add(line);
        }
        input.close();
        
        Assert.assertEquals(ret.size(), expected.size());
        for (int i=0; i<ret.size(); i++) {
            Assert.assertEquals(ret.get(i), expected.get(i));
        }
    } catch (Exception e) {
        e.printStackTrace(System.out);
        Assert.assertTrue("Exception " + e.getLocalizedMessage(), false);
    }
    }

    @Test
    public void TestTail_largeBuffer() {
    try{
        Vector<String> ret = Utils.tail(
                                        "src/unit/azkaban/common/utils/test_tail_file.txt", 
                                        14, 2000);
        
        Vector<String> expected = new Vector<String>();
        BufferedReader input = 
            new BufferedReader(new FileReader("src/unit/azkaban/common/utils/test_tail_file_expected.txt"));

        String line = null;
        while ((line = input.readLine()) != null) {
            expected.add(line);
        }
        input.close();
        
        Assert.assertEquals(ret.size(), expected.size());
        for (int i=0; i<ret.size(); i++) {
            Assert.assertEquals(ret.get(i), expected.get(i));
        }
    } catch (Exception e) {
        e.printStackTrace(System.out);
        Assert.assertTrue("Exception " + e.getLocalizedMessage(), false);
    }
    }
}
