package edu.auth.jetproud.proud.sink;

import com.hazelcast.jet.contrib.influxdb.InfluxDbSinks;
import com.hazelcast.jet.pipeline.Sink;
import edu.auth.jetproud.application.config.InfluxDBConfiguration;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class ProudInfluxDBSink extends AnyProudSink<Point>
{

    public ProudInfluxDBSink(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public Sink<Point> createJetSink() {
        InfluxDBConfiguration config = proudContext.influxDBConfiguration();

        return InfluxDbSinks.influxDb(
                config.getHost(),
                config.getDatabase(),
                config.getUser(),
                config.getPassword()
        );
    }

    @Override
    public Point convertResultItem(long key, OutlierQuery query) {
        final String measurement = "outliers";
        long timestamp = key; // Is it a millisecond timestamp ???

        HashMap<String,String> tags = new HashMap<>();
        tags.put("W", String.valueOf(query.window));
        tags.put("S", String.valueOf(query.slide));
        tags.put("k", String.valueOf(query.kNeighbours));
        tags.put("R", String.valueOf(query.range));

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("Outliers", query.outlierCount);

        return Point.measurement(measurement)
                .time(timestamp, TimeUnit.MILLISECONDS)
                .tag(tags)
                .fields(fields)
                .build();
    }
}
