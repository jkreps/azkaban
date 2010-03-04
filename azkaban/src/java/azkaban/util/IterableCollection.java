package azkaban.util;

import java.util.AbstractCollection;
import java.util.Iterator;

/**
 *
 */
public class IterableCollection<T> extends AbstractCollection<T>
{
    private final Iterable<T> baseIterable;
    private final int size;

    public IterableCollection(Iterable<T> baseIterable)
    {
        this(baseIterable, -1);
    }

    public IterableCollection(Iterable<T> baseIterable, int size)
    {
        this.baseIterable = baseIterable;
        this.size = size;
    }

    @Override
    public Iterator<T> iterator()
    {
        return baseIterable.iterator();
    }

    @Override
    public int size()
    {
        if (size >= 0) {
            return size;
        }
        else {
            throw new UnsupportedOperationException(String.format("size[%s] is below 0 == no determinable size."));
        }
    }
}
