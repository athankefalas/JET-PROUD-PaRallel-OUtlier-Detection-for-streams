package edu.auth.jetproud.application.parameters.data;

import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;

import java.util.Arrays;
import java.util.List;

import static edu.auth.jetproud.application.parameters.data.ProudSpaceOption.*;

public enum ProudAlgorithmOption {

    // User Defined
    UserDefined(null, Lists.of(Single, MultiQueryMultiParams, MultiQueryMultiParamsMultiWindowParams)),

    // Single Query Space
    Naive("naive"),
    Advanced("advanced"),
    AdvancedExtended("advanced_extended"),
    Slicing("slicing"),
    PMCod("pmcod"),
    PMCodNet("pmcod_net"),
    // Multi Query Space
    AMCod("amcod", Lists.of(MultiQueryMultiParams)),
    // Multi Query + Multi Query Multi Window Spaces
    Sop("sop", Lists.of(MultiQueryMultiParams, MultiQueryMultiParamsMultiWindowParams)),
    PSod("psod", Lists.of(MultiQueryMultiParams, MultiQueryMultiParamsMultiWindowParams)),
    PMCSky("pmcsky", Lists.of(MultiQueryMultiParams, MultiQueryMultiParamsMultiWindowParams));


    private String value;
    private List<ProudSpaceOption> supportedSpaces;

    ProudAlgorithmOption(String value) {
        this(value, Lists.of(Single));
    }

    ProudAlgorithmOption(String value, List<ProudSpaceOption> supportedSpaces) {
        this.value = value;
        this.supportedSpaces = Lists.copyOf(supportedSpaces);
    }

    public boolean isSupportedInSpace(ProudSpaceOption spaceOption) {
        return supportedSpaces.stream().anyMatch((space)->space == spaceOption);
    }

    public static Parser<ProudAlgorithmOption> parser() {
        return (value) -> Arrays.stream(values())
                .filter((it)->it.value != null)
                .filter((it)->it.value.equals(value.toLowerCase()))
                .findFirst()
                .orElse(null);
    }
}
