package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.source.streams.StreamGenerator;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.util.List;

public class ProudStreamSource implements ProudSource<AnyProudData>
{

    private ProudContext proudContext;
    private StreamGenerator streamGenerator;

    public ProudStreamSource(ProudContext proudContext, StreamGenerator streamGenerator) {
        this.proudContext = proudContext;
        this.streamGenerator = streamGenerator;
    }


    @Override
    public StreamStage<AnyProudData> readInto(Pipeline pipeline) {
        long allowedLag = proudContext.internalConfiguration().getAllowedLateness();

        return pipeline.readFrom(createJetSource())
                .withTimestamps((it)->it.arrival, allowedLag);
    }

    private StreamSource<AnyProudData> createJetSource() {
        return SourceBuilder
                .stream("proud-stream-source", ctx -> new Context(streamGenerator))
                .fillBufferFn(Context::appendTo)
                .destroyFn(Context::close)
                .build();
    }

    public static class Context implements StreamGenerator, Serializable
    {
        private StreamGenerator streamGenerator;

        public Context(StreamGenerator streamGenerator) {
            this.streamGenerator = streamGenerator;
        }

        public void appendTo(SourceBuilder.SourceBuffer<AnyProudData> buffer) {
            boolean isComplete = isComplete();
            List<AnyProudData> items = availableItems();

            if (items == null) // Null safety
                items = Lists.make();

            for (AnyProudData item:items) {
                buffer.add(item);
            }

            if (isComplete) {
                buffer.close();
            }
        }

        @Override
        public List<AnyProudData> availableItems() {
            return streamGenerator.availableItems();
        }

        @Override
        public boolean isComplete() {
            return streamGenerator.isComplete();
        }

        @Override
        public void close() {
            streamGenerator.close();
            streamGenerator = null;
        }
    }

}
