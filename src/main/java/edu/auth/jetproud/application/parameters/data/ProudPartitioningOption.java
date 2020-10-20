package edu.auth.jetproud.application.parameters.data;

import edu.auth.jetproud.utils.Parser;

import java.util.Arrays;

public enum ProudPartitioningOption
{
    Replication("replication"),
    Grid("grid"),
    Tree("tree");


    private String value;

    ProudPartitioningOption(String value) {
        this.value = value;
    }

    public static Parser<ProudPartitioningOption> parser() {
        return (value) -> Arrays.stream(values())
                .filter((it)->it.value.equals(value.toLowerCase()))
                .findFirst()
                .orElse(null);
    }
}
