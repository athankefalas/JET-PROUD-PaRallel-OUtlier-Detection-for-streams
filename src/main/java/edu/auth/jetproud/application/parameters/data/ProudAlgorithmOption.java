package edu.auth.jetproud.application.parameters.data;

import edu.auth.jetproud.utils.Parser;

import java.util.Arrays;

public enum ProudAlgorithmOption {

    // Single Query Space
    Naive("naive"),
    Advanced("advanced"),
    AdvancedExtended("advanced_extended"),
    Slicing("slicing"),
    PMCod("pmcod"),
    PMCodNet("pmcod_net"),
    // Multi Query Space
    AMCod("amcod", true),
    Sop("sop", true),
    PSod("psod", true),
    PMCSky("pmcsky", true);


    private String value;
    private boolean isMultiQueryAlgorithm;

    ProudAlgorithmOption(String value) {
        this(value, false);
    }

    ProudAlgorithmOption(String value, boolean isMultiQueryAlgorithm) {
        this.value = value;
        this.isMultiQueryAlgorithm = isMultiQueryAlgorithm;
    }

    public static Parser<ProudAlgorithmOption> parser() {
        return (value) -> Arrays.stream(values())
                .filter((it)->it.value.equals(value.toLowerCase()))
                .findFirst()
                .orElse(null);
    }
}
