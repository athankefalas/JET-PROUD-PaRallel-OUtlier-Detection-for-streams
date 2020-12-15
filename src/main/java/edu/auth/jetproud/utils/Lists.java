package edu.auth.jetproud.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

public final class Lists
{

    // Prevent instantiation
    private Lists(){}

    public static <E> List<E> make()
    {
        return new LinkedList<>();
    }

    public static <E> List<E> make(int capacity)
    {
        if (capacity <= 0)
            return new ArrayList<>();
        else if (capacity <= 256)
            return new ArrayList<>(capacity);
        else
            return new LinkedList<>();
    }

    public static <E> LinkedList<E> makeLinkedList()
    {
        return new LinkedList<>();
    }

    public static <E> LinkedList<E> makeLinkedList(int capacity)
    {
        return new LinkedList<>();
    }

    public static <E> ArrayList<E> makeArrayList()
    {
        return new ArrayList<>();
    }

    public static <E> ArrayList<E> makeArrayList(int capacity)
    {
        if (capacity <= 0)
            return new ArrayList<>();
        else if (capacity <= 256)
            return new ArrayList<>(capacity);
        else
            return new ArrayList<>();
    }

    @SafeVarargs
    public static <E> List<E> of(E first, E ... others)
    {
        int capacity = first != null ? 1 : 0;

        if (others != null)
        {
            capacity += others.length;
        }

        if (capacity == 0)
            return make();

        List<E> list = make(capacity);

        if (first != null)
            list.add(first);

        if (others != null)
        {
            for (E element:others)
            {
                if (element != null)
                    list.add(element);
            }
        }

        return list;
    }

    public static <E> E firstIn(List<E> list)
    {
        if (list == null)
            return null;

        if (list.size() > 0)
            return list.get(0);

        return null;
    }

    public static <E> List<E> cast(Object any)
    {
        try
        {
            return (List<E>) any;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static <E> List<E> from(E[] array)
    {
        List<E> list = make(array.length);

        for (E element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Boolean> from(boolean[] array)
    {
        List<Boolean> list = make(array.length);

        for (Boolean element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Character> from(char[] array)
    {
        List<Character> list = make(array.length);

        for (Character element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Byte> from(byte[] array)
    {
        List<Byte> list = make(array.length);

        for (Byte element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Short> from(short[] array)
    {
        List<Short> list = make(array.length);

        for (Short element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Integer> from(int[] array)
    {
        List<Integer> list = make(array.length);

        for (Integer element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Long> from(long[] array)
    {
        List<Long> list = make(array.length);

        for (Long element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Float> from(float[] array)
    {
        List<Float> list = make(array.length);

        for (Float element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public static List<Double> from(double[] array)
    {
        List<Double> list = make(array.length);

        for (Double element:array)
        {
            if (element != null)
                list.add(element);
        }

        return list;
    }

    public synchronized static <E> List<E> copyOf(Collection<E> collection)
    {
        int size = collection != null ? collection.size() : 1024;
        List<E> copy = make(size);

        if (collection != null)
            copy.addAll(collection);

        return copy;
    }

    public static <E> List<E> reversing(Collection<E> collection) {
        return reverse( copyOf(collection) );
    }

    @SafeVarargs
    public static <E> List<E> join(List<E> first, List<E> second, List<E> ...others)
    {
        List<E> joined = Lists.make();

        if (first!=null)
            joined.addAll(first);

        if (second!=null)
            joined.addAll(second);

        if (others != null)
        {
            for (List<E> arg: others)
            {
                if (arg!=null)
                    joined.addAll(arg);
            }
        }

        return joined;
    }

    @SafeVarargs
    public static <E> List<E> distinctJoin(List<E> first, List<E> second, List<E> ...others)
    {
        List<E> joined = Lists.make();

        if (first!=null)
            first.forEach(it->{
                if (!joined.contains(it))
                    joined.add(it);
            });

        if (second!=null)
            second.forEach(it->{
                if (!joined.contains(it))
                    joined.add(it);
            });

        if (others != null)
        {
            for (List<E> arg: others)
            {
                if (arg!=null)
                    arg.forEach(it->{
                        if (!joined.contains(it))
                            joined.add(it);
                    });
            }
        }

        return joined;
    }

    public static <E> List<E> reverse(List<E> list)
    {
        if (list == null)
            return make();

        if (list.size() == 1)
        {
            return of(list.get(0));
        }

        List<E> reversed = make(list.size());

        for (int i=list.size()-1; i >= 0; i--)
        {
            E element = list.get(i);
            reversed.add(element);
        }

        return reversed;
    }

    public static <E> List<List<E>> splitInBatches(List<E> list, int batchSize)
    {
        List<List<E>> batches = make();

        if (list == null)
            return batches;

        if (list.size() < batchSize)
        {
            batches.add(list);
            return batches;
        }

        List<E> batch = make();
        int idx = 0;
        boolean done = false;

        while(!done)
        {
            while(batch.size() < batchSize)
            {
                if (idx >= list.size())
                {
                    done = true;
                    break;
                }

                batch.add(list.get(idx++));
            }

            if (batch.size() > 0)
                batches.add(batch);
            batch = make();
        }

        return batches;
    }

    public static <E> List<List<E>> splitBy(List<E> list, Predicate<E> predicate)
    {
        List<List<E>> split = make();

        List<E> matches = make();
        List<E> nonMatches = make();

        split.add(matches);
        split.add(nonMatches);

        if (list == null || list.size() < 1)
        {
            return split;
        }

        if (predicate == null)
        {
            matches.addAll(list);
            return split;
        }

        for (E element:list)
        {
            if (predicate.test(element))
                matches.add(element);
            else
                nonMatches.add(element);
        }

        return split;
    }

    public static List<Integer> range(int from, int to) {
        List<Integer> range = make();

        for(int r=from;r<to;r++) {
            range.add(r);
        }

        return range;
    }

    public static List<Integer> closedRange(int from, int to) {
        List<Integer> range = make();

        for(int r=from;r<=to;r++) {
            range.add(r);
        }

        return range;
    }

    public static <T> T getAtOrNull(List<T> list, int index) {
        if (index < 0 || index >= list.size())
            return null;

        return list.get(index);
    }

}

