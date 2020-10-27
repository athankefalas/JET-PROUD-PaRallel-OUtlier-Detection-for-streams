package edu.auth.jetproud.proud;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;

public final class ProudExecutor
{

    private ProudExecutor(){}

    public static Job execute(ProudContext context, Pipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();

        Job job = jet.newJob(pipeline);
        job.join();

        return job;
    }
}
