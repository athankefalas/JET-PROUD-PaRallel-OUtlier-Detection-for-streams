package edu.auth.jetproud;

import com.hazelcast.jet.Job;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.source.ProudSource;

public class ApplicationMain {

    public static void main(String[] args) throws Exception {
        Proud proud = Proud.builder(args)
                .build();

        ProudPipeline pipeline = ProudPipeline.create(proud);

        // Auto read
        pipeline.readData();

        pipeline.readFrom(ProudSource.debugFileSource(proud))
                .partition()
                .detectOutliers()
                .sinkData();
                // or .writeTo(ProudSink.auto(proud));


        // Case 1 - Native proud
        //pipeline.readFrom( ... ProudSource ( ? extends StreamSource ) ... )
        //    .applyPartitioning()
        //    .detectOutliers() | .proudExecute()
        //    .writeOutliersTo( ... Sink ...)    { .writeTo( Sinks.influxDB() ) }

        // Case 2 - Any ProudDataConvertible Stage
        //pipeline.readFrom( ... StreamSource ... )
        //    .mappedToProudData()
        //    .applyPartitioning()
        //    .detectOutliers() | .proudExecute()
        //    .writeOutliersTo( ... Sink ...)    { .writeTo( Sinks.influxDB() ) }

        Job job = ProudExecutor.executeJob(pipeline);
    }
}
