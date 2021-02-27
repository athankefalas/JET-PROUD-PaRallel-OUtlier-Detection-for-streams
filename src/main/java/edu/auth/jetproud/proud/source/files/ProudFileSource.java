package edu.auth.jetproud.proud.source.files;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.test.TestSources;
import edu.auth.jetproud.exceptions.ParserDeserializationException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.partitioning.ProudPartitioning;
import edu.auth.jetproud.proud.partitioning.ReplicationPartitioning;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Tuple;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class ProudFileSource<T extends AnyProudData> implements ProudSource<T>, Serializable {

    private final ProudContext proudContext;
    private String fileName = "input_20k.txt";
    private final Parser<T> parser;

    public ProudFileSource(ProudContext proudContext, Parser<T> parser) {
        this.proudContext = proudContext;
        this.parser = parser;
    }

    public ProudFileSource(ProudContext proudContext, String fileName, Parser<T> parser) {
        this.proudContext = proudContext;
        this.fileName = fileName;
        this.parser = parser;
    }

    private String filePath() {
        String datasetHomeDir = proudContext.datasetConfiguration().getDatasetHome();

        Path path = Paths.get(datasetHomeDir, fileName);
        return path.toString();
    }


    @Override
    public StreamStage<T> readInto(Pipeline pipeline) {
        long allowedLag = proudContext.internalConfiguration().getAllowedLateness();

        // Pre-read items from file and emit through a List
        List<T> all = readItems();

        return pipeline.readFrom(listJetSource(all))
                .addTimestamps((it)->it.arrival, allowedLag);
    }

    private List<T> readItems() {
        List<T> allItems = Lists.make();

        try {
            File file = new File(filePath());
            InputStreamReader inputStreamReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line = "";

            while (true) {
                line = reader.readLine();

                if (line == null) {
                    reader.close();
                    break;
                }

                if (line.trim().length() == 0)
                    continue;

                T parsedObject = parser.parseString(line);

                if (parsedObject == null)
                    throw new IllegalArgumentException("Unreadable object.");

                allItems.add(parsedObject);
            }
        } catch (Exception e) {
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println(e.getLocalizedMessage());
            e.printStackTrace();
        }

        return allItems;
    }

    // List Input Source

    private BatchSource<T> listJetSource(List<T> items) {
        // Default Test Source
        //return TestSources.items(items);

        // Custom Batch Source
        return SourceBuilder.batch("proud-source-ls", (ctx)-> new ListItemsContext<>(ctx,items))
                .fillBufferFn(ListItemsContext<T>::fill)
                .destroyFn(ListItemsContext<T>::close)
                .distributed(1)
                .build();
    }

    // Parser Factory

    public static Parser<AnyProudData> proudDataParser(String fieldDelimiter, String valueDelimiter) {
        return (string) -> {
            final Parser<Integer> idParser = Parser.ofInt();
            final Parser<List<Double>> valueParser = Parser.ofDoubleList(valueDelimiter);

            try {
                String[] fields = string.split(fieldDelimiter);
                int id = idParser.parseString(fields[0]);
                List<Double> value = valueParser.parseString(fields[1]);

                if (value == null || value.stream().allMatch(Objects::isNull))
                    throw new ParserDeserializationException(fields[1], "List<Double>");

                return new AnyProudData(id, value, id, 0);
            } catch (Exception e) {
                return null;
            }
        };
    }

    //// Contexts

    private static class ListItemsContext<T extends AnyProudData> implements Serializable {
        private static final int BATCH_SIZE = 2048;

        private Processor.Context processorContext;
        private LinkedList<T> items;
        private int position = 0;

        public ListItemsContext(Processor.Context processorContext, List<T> items) {
            this.processorContext = processorContext;
            this.items = new LinkedList<>(items);
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public void fill(SourceBuilder.SourceBuffer<T> buf) throws Exception {
            int emittedItemCount = 0;

            while (position >= 0) {

                if (BATCH_SIZE > 0) {
                    if (emittedItemCount >= BATCH_SIZE) {
                        break;
                    }
                }

                if (position >= items.size()) {
                    buf.close();
                    close();
                    break;
                }

                T item = items.get(position);
                buf.add(item);
                emittedItemCount++;

                position++;
            }
        }

        public void close() {
            items.clear();
            position = -1;
        }

    }
}
