package edu.auth.jetproud.utils;

import java.util.*;
import java.util.stream.Collectors;

public final class CollectionUtils {

    // Prevent instantiation
    private CollectionUtils(){}


    /**
     * Identifies the minimum and maximum elements from an iterable, according
     * to the natural ordering of the elements.
     * @param items An {@link Iterable} object with the elements
     * @param <T> The type of the elements.
     * @return A pair with the minimum and maximum elements.
     */
    public static <T extends Comparable<T>> Pair<T> minMax(Iterable<T> items) {
        Iterator<T> iterator = items.iterator();

        if(!iterator.hasNext()) {
            return null;
        }

        T min = iterator.next();
        T max = min;

        while(iterator.hasNext()) {
            T item = iterator.next();

            if(item.compareTo(min) <= 0) {
                min = item;
            }
            if(item.compareTo(max) >= 0) {
                max = item;
            }
        }

        return new Pair<>(min, max);
    }

    /**
     * Randomly chooses elements from the collection.
     * @param collection The collection.
     * @param n The number of elements to choose.
     * @param <T> The type of the elements.
     * @return A list with the chosen elements.
     */
    public static <T> List<T> randomSample(Collection<T> collection, int n) {
        int sampleSize = n;
        Random random = new Random();

        List<T> items = Lists.copyOf(collection);
        List<Integer> sampleIndices = Lists.make(n);

        if (sampleSize == 0 || items.isEmpty())
            return Lists.make();

        if (sampleSize > items.size())
            sampleSize = items.size();

        while (sampleIndices.size() < sampleSize) {
            int index = random.nextInt(items.size());

            if (sampleIndices.contains(index))
                continue;

            sampleIndices.add(index);
        }

        return sampleIndices.stream()
                .map(items::get)
                .collect(Collectors.toList());
    }

}
