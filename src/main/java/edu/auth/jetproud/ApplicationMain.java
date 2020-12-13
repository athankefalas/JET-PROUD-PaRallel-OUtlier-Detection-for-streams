package edu.auth.jetproud;

import com.hazelcast.jet.Job;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ApplicationMain {

    public static void main(String[] args) throws Exception {
        Proud proud = Proud.builder(args)
                .build();

        ProudPipeline pipeline = ProudPipeline.create(proud);

        // Auto read
        pipeline.readData();

        pipeline.readFrom(ProudSource.file(proud))
                .partition()
                .detectOutliers()
                .sinkData();
                // or .writeTo(ProudSink.auto(proud));

        Job job = ProudExecutor.executeJob(pipeline);
    }
}
