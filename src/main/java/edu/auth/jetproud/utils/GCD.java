package edu.auth.jetproud.utils;

import java.util.List;
import java.util.Optional;

public final class GCD {

    // Prevent instantiation
    private GCD(){}

    private static int gcd(int a, int b) {
        return a == 0 ? b : gcd(b % a,a);
    }

    public static Optional<Integer> ofElementsIn(List<Integer> elements) {

        if (elements.isEmpty())
            return Optional.empty();

        Integer result = elements.get(0);

        for (Integer element: elements) {
            result = gcd(element, result);
        }

        return Optional.of(result);
    }

}
