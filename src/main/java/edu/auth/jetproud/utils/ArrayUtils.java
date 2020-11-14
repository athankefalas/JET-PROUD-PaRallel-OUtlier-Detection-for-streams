package edu.auth.jetproud.utils;

import java.util.Arrays;

public class ArrayUtils {

    public static int[][] multidimensionalWith(int value, int one, int two) {
        int[][] array = new int[one][two];

        for (int i=0;i<array.length;i++) {
            Arrays.fill(array[i], value);
        }

        return array;
    }

    public static int[][][] multidimensionalWith(int value, int one, int two, int three) {
        int[][][] array = new int[one][two][three];

        for (int i=0;i<array.length;i++) {
            for (int j=0; j < array[i].length; j++) {
                Arrays.fill(array[i][j], value);
            }
        }

        return array;
    }

}
