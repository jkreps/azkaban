package azkaban.utils;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import azkaban.util.CircularBuffer;

import static org.junit.Assert.*;

public class CircularBufferTest {

    @Test
    public void testBuffer() {
        CircularBuffer<Integer> buffer = new CircularBuffer<Integer>(5);
        insert(buffer, 0, 1, 2, 3);
        check(buffer, 0, 1, 2, 3);
        insert(buffer, 4);
        check(buffer, 0, 1, 2, 3, 4);
        insert(buffer, 5);
        check(buffer, 1, 2, 3, 4, 5);
        insert(buffer, 6, 7, 8, 9, 10);
        check(buffer, 6, 7, 8, 9, 10);
    }
    
    private void insert(CircularBuffer<Integer> buffer, int...items) {
        for(int item: items)
            buffer.append(item);
    }
    
    private void check(CircularBuffer<Integer> buffer, int...items) {
        assertEquals("Buffer size should be " + items.length, items.length, buffer.getSize());
        List<Integer> bufferCopy = new ArrayList<Integer>();
        for(int num: buffer)
            bufferCopy.add(num);
        List<Integer> itemsCopy = new ArrayList<Integer>();
        for(int num: buffer)
            itemsCopy.add(num);
        assertEquals(itemsCopy, bufferCopy);
    }
    
}
