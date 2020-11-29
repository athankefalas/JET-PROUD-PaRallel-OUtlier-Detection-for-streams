package edu.auth.jetproud.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Parser<T> extends Serializable {

    T parseString(String value);

    static Parser<Boolean> ofBoolean() {
        List<String> trueValues = Lists.of("1","yes","y","true", "t");
        List<String> falseValues = Lists.of("0","no","n","false", "f");

        return value -> {

            if (value == null)
                return null;

            String sanitizedValue = value.trim().toLowerCase();

            if (trueValues.stream().anyMatch((it)->it.equals(sanitizedValue))) {
                return true;
            } else if(falseValues.stream().anyMatch((it)->it.equals(sanitizedValue))) {
                return false;
            } else {
                return null;
            }
        };
    }

    static Parser<Integer> ofInt() {
        return value -> {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                return null;
            }
        };
    }

    static Parser<Long> ofLong() {
        return value -> {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException nfe) {
                return null;
            }
        };
    }

    static Parser<Float> ofFloat() {
        return value -> {
            try {
                return Float.parseFloat(value);
            } catch (NumberFormatException nfe) {
                return null;
            }
        };
    }

    static Parser<Double> ofDouble() {
        return value -> {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException nfe) {
                return null;
            }
        };
    }

    static Parser<String> ofString() {
        return value -> value;
    }

    static <E extends Enum<E>> Parser<E> ofEnum(Class<E> enumClass) {
        return value -> {
            try{
                return E.valueOf(enumClass, value);
            } catch (Exception e) {
                return null;
            }
        };
    }

    // List Variants

    static Parser<List<Boolean>> ofBooleanList(String delimiter) {
        return (value) -> {
            Parser<Boolean> parser = Parser.ofBoolean();
            return Arrays.stream(value.split(delimiter))
                    .map(String::trim)
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static Parser<List<Integer>> ofIntList(String delimiter) {
        return (value) -> {
            Parser<Integer> parser = Parser.ofInt();
            return Arrays.stream(value.split(delimiter))
                    .map(String::trim)
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static Parser<List<Long>> ofLongList(String delimiter) {
        return (value) -> {
            Parser<Long> parser = Parser.ofLong();
            return Arrays.stream(value.split(delimiter))
                    .map(String::trim)
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static Parser<List<Float>> ofFloatList(String delimiter) {
        return (value) -> {
            Parser<Float> parser = Parser.ofFloat();
            return Arrays.stream(value.split(delimiter))
                    .map(String::trim)
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static Parser<List<Double>> ofDoubleList(String delimiter) {
        return (value) -> {
            Parser<Double> parser = Parser.ofDouble();
            return Arrays.stream(value.split(delimiter))
                    .map(String::trim)
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static Parser<List<String>> ofStringList(String delimiter) {
        return (value) -> {
            Parser<String> parser = Parser.ofString();
            return Arrays.stream(value.split(delimiter))
                    .map(parser::parseString)
                    .collect(Collectors.toList());
        };
    }

    static <T,S> Parser<S> using(final Parser<T> original, Function<T, S> converter) {
        return (value) -> converter.apply(original.parseString(value));
    }
}
