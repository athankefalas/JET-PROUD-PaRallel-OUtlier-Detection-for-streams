package edu.auth.jetproud.utils;

public final class Utils
{

    private Utils(){}

    public static <T> T firstNonNull(T one, T ...others) {
        if (one != null)
            return one;

        if (others == null)
            return null;

        for (T item:others) {
            if (item != null)
                return item;
        }

        return null;
    }

}
