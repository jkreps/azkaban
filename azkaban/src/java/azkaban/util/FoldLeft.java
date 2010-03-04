package azkaban.util;

import java.util.Iterator;

/**
 *
 */
public abstract class FoldLeft<InType, OutType>
{

    public static <TheType> TheType fold(Iterable<TheType> input, FoldLeft<TheType, TheType> reducer)
    {
        return fold(input.iterator(), reducer);
    }

    public static <TheType> TheType fold(Iterator<TheType> input, FoldLeft<TheType, TheType> reducer)
    {
        if (! input.hasNext()) {
            throw new IllegalArgumentException("Iterable was over an iterator with no elements, " +
                                               "that doesn't work without a base object.");
        }
        TheType firstVal = input.next();

        if (! input.hasNext()) {
            return firstVal;
        }

        TheType secondVal = input.next();

        return fold(input, reducer.fold(firstVal, secondVal), reducer);
    }

    public static <InType, OutType> OutType fold(Iterable<InType> input, OutType base, FoldLeft<InType, OutType> reducer)
    {
        return fold(input.iterator(), base, reducer);
    }

    public static <InType, OutType> OutType fold(Iterator<InType> input, OutType base, FoldLeft<InType, OutType> reducer)
    {
        OutType currBase = base;
        while (input.hasNext()) {
            currBase = reducer.fold(currBase, input.next());
        }

        return currBase;
    }

    public abstract OutType fold(OutType oldValue, InType newValue);
}
