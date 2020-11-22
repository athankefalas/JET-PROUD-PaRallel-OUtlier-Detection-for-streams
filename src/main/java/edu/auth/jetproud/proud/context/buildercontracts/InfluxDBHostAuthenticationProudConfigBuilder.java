package edu.auth.jetproud.proud.context.buildercontracts;

public interface InfluxDBHostAuthenticationProudConfigBuilder {

    default DebugSelectionProudConfigBuilder authenticatedWith(String user) {
        return authenticatedWith(user, "");
    }

    DebugSelectionProudConfigBuilder authenticatedWith(String user, String password);
}
