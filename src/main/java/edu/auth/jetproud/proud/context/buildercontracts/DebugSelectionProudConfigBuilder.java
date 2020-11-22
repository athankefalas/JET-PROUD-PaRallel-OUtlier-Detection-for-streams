package edu.auth.jetproud.proud.context.buildercontracts;

import edu.auth.jetproud.proud.context.Proud;

public interface DebugSelectionProudConfigBuilder {
    BuildableProudConfigBuilder enablingDebug();

    Proud build();
}
