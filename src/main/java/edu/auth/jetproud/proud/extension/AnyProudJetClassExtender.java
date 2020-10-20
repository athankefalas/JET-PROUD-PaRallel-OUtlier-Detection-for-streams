package edu.auth.jetproud.proud.extension;

import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.extension.ClassExtender;

import java.util.UUID;

public abstract class AnyProudJetClassExtender<Target> implements ClassExtender<Target>
{
    private static final UUID extenderId = UUID.randomUUID();
    private static final String extenderName = "JetProudImplementationProvider";

    protected Target target;
    protected ProudContext proudContext;

    public AnyProudJetClassExtender(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    @Override
    public UUID extenderId() {
        return extenderId;
    }

    @Override
    public String extenderName() {
        return extenderName;
    }

    @Override
    public final void setTarget(Target target) {
        this.target = target;
    }

}
