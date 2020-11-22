package edu.auth.jetproud.proud;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import edu.auth.jetproud.proud.context.ProudContext;

public final class ProudExecutor
{

    private ProudExecutor(){}

    public static Job createJob(Pipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();

        return jet.newJob(pipeline);
    }

    public static Job executeJob(Pipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();

        Job job = jet.newJob(pipeline);
        job.join();

        return job;
    }
}
