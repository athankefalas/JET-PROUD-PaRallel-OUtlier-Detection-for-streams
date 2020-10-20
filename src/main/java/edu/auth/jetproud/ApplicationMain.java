package edu.auth.jetproud;

import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudPartitionedStreamStage;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.proud.Proud;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.utils.Tuple;

import java.util.List;

public class ApplicationMain {

    enum Temp
    {
        One, Two
    }

    private static <T> void testParse(List<String> items, Parser<T> parser) {
        for (String item : items) {
            T value = parser.parseString(item);

            if (value == null) {
                System.out.println("Parsed item '"+item+"' as (null)");
            } else {
                System.out.println("Parsed item '"+item+"' as ("+value.toString()+")");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Proud proud = Proud.builder(args)
                .build();

        ProudPipeline pipeline = ProudPipeline.create(proud);
        ProudPartitionedStreamStage<AnyProudData> s = pipeline.readFrom(ProudSource.debugFileSource(proud))
            .partitioned();

        long windowSize = 0;
        long windowSlideSize = 0;

        s.map((it)-> new Tuple<>(it.getPartition(), it.getData()))
                .groupingKey(Tuple::getFirst)
                .window(WindowDefinition.sliding(windowSize,windowSlideSize))
                .distinct()
                ;



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

        System.out.println();
    }
}
