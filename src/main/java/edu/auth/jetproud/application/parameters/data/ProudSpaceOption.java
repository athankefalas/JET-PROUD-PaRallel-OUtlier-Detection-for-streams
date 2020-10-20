package edu.auth.jetproud.application.parameters.data;

import edu.auth.jetproud.utils.Parser;

import java.util.Arrays;

public enum ProudSpaceOption {
    None(""),
    Single("single"),
    MultiQueryMultiParams("rk"),
    MultiQueryMultiParamsMultiWindowParams("rkws");


    private String value;

    ProudSpaceOption(String value) {
        this.value = value;
    }

    public static Parser<ProudSpaceOption> parser() {
        return (value) -> Arrays.stream(values())
                .filter((it)->it.value.equals(value.toLowerCase()))
                .findFirst()
                .orElse(null);
    }

}
