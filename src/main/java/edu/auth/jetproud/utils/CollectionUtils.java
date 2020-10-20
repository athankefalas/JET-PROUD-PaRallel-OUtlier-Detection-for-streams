package edu.auth.jetproud.utils;

import java.util.*;

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
            if(item.compareTo(min) < 0) {
                min = item;
            }
            if(item.compareTo(max) > 0) {
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
        Random random = new Random();

        List<T> list = Lists.copyOf(collection);
        List<T> sample = new ArrayList<>(n);

        while(n > 0  &&  !list.isEmpty()) {
            int index = random.nextInt(list.size());
            sample.add(list.get(index));

            int indexLast = list.size() - 1;
            T last = list.remove(indexLast);

            if(index < indexLast) {
                list.set(index, last);
            }

            n--;
        }

        return sample;
    }

}
