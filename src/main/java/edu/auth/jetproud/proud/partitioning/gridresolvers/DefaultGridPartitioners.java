package edu.auth.jetproud.proud.partitioning.gridresolvers;

import edu.auth.jetproud.proud.partitioning.GridPartitioning;

public final class DefaultGridPartitioners {

    private DefaultGridPartitioners(){}

    public static GridPartitioning.GridPartitioner forDatasetNamed(String name) {
        switch(name.trim().toLowerCase()) {
            case "stk":
                return new StockGridPartitioner();
            case "tao":
                return new TAOGridPartitioner();
        }

        return null;
    }

}
