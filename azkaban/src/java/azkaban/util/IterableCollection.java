/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
