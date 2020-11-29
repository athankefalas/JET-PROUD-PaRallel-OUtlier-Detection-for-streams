package edu.auth.jetproud.proud;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;

public final class ProudExecutor
{

    private ProudExecutor(){}

    public static ProudJob createJob(ProudPipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();
        Job job = jet.newJob(pipeline);

        return new ProudJob(pipeline.proudContext(), jet, job);
    }

    public static ProudJob executeJob(ProudPipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();
        Job job = jet.newJob(pipeline.jetPipeline());

        ProudJob proudJob = new ProudJob(pipeline.proudContext(), jet, job);
        proudJob.join();

        return proudJob;
    }
}
