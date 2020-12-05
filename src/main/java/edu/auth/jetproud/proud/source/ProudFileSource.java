package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.test.TestSources;
import edu.auth.jetproud.exceptions.ParserDeserializationException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class ProudFileSource<T extends AnyProudData> implements ProudSource<T>, Serializable {

    private ProudContext proudContext;
    private String fileName = "input_20k.txt";
    private Parser<T> parser;

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
        String dataset = proudContext.configuration().getDataset();

        Path path = Paths.get(datasetHomeDir, fileName);
        return path.toString();
    }


    @Override
    public StreamStage<T> readInto(Pipeline pipeline) {
        long allowedLag = proudContext.internalConfiguration().getAllowedLateness();

        // Use pre-made Jet files source - and create a /JetInput directory
        // to force single file reading.
        String datasetHomeDir = proudContext.datasetConfiguration().getDatasetHome();
        String dataset = proudContext.configuration().getDataset();

        Path path = Paths.get(datasetHomeDir, "JetInput");

//        return pipeline.readFrom(Sources.files(path.toString()))
//                .map(parser::parseString)
//                .addTimestamps((it)->it.arrival, allowedLag);

        // Pre-read items and emit through a List - stops by some kind of timeout after 16s
        List<T> all = readItems();

        return pipeline.readFrom(listJetSource(all))
                .addTimestamps((it)->it.arrival, allowedLag);

        // Read from file with custom source - stops by some kind of timeout after 16s
//        return pipeline.readFrom(createJetSource())
//                .withTimestamps((it)->it.arrival, allowedLag);
    }

    private List<T> readItems() {
        List<T> allItems = Lists.make();

        try {
            File file = new File(filePath());
            InputStreamReader inputStreamReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line = "";

            while (line != null) {
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

    // File Input Source

    private FileBufferedReaderContext<T> fileBufferedReaderContext(Processor.Context ctx) throws FileNotFoundException {
        String datasetHomeDir = proudContext.datasetConfiguration().getDatasetHome();
        String dataset = proudContext.configuration().getDataset();

        Path path = Paths.get(datasetHomeDir, fileName);
        return new FileBufferedReaderContext<>(path.toString(), parser);
    }

    private StreamSource<T> createJetSource() {
        return SourceBuilder.timestampedStream("proud-source", this::fileBufferedReaderContext)
                .fillBufferFn(FileBufferedReaderContext<T>::fill)
                .destroyFn(FileBufferedReaderContext::close)
                .build().setPartitionIdleTimeout(0);
    }

    // List Input Source

    private BatchSource<T> listJetSource(List<T> items) {
        return TestSources.items(items);

//        return SourceBuilder.batch("proud-source-ls", (ctx)-> new ListItemsContext<>(ctx,items))
//                .fillBufferFn(ListItemsContext<T>::fill)
//                .destroyFn(ListItemsContext<T>::close)
//                .build();
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

                return new AnyProudData(id, value, id, 0);
            } catch (Exception e) {
                return null;
            }
        };
    }

    //// Contexts

    private static class ListItemsContext<T extends AnyProudData> implements Serializable {
        private static final int BATCH_SIZE = 128;

        private Processor.Context processorContext;
        private LinkedList<T> items;
        private int position = 0;

        public ListItemsContext(Processor.Context processorContext, List<T> items) {
            //System.out.println("Created ListItemsContext for "+items.size()+" items.");
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
                if (emittedItemCount >= BATCH_SIZE) {
                    break;
                }

                if (position >= items.size()) {
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
            System.out.println("\n\n\n\n!!! Batch closed. POS: "+position+" TOTAL: "+items.size()+"\n\n\n\n");
            items.clear();
            position = -1;
        }

    }

    private static class FileBufferedReaderContext<T extends AnyProudData> implements Serializable {
        private String filePath;
        private Parser<T> parser;
        private transient BufferedReader reader;
        private DistributedMap<String, Boolean> state = new DistributedMap<>("SOURCE_STATE");

        FileBufferedReaderContext(String filePath, Parser<T> parser) {
            this.filePath = filePath;
            this.parser = parser;
        }

        private BufferedReader getReader() {
            if (reader == null) {

                state.put("DONE", false);

                try {
                    File file = new File(filePath);
                    InputStreamReader inputStreamReader = new FileReader(file);
                    this.reader = new BufferedReader(inputStreamReader);
                } catch (Exception e) {
                    System.err.println(e.getLocalizedMessage());
                    e.printStackTrace();
                    this.reader = null;
                }
            }

            return reader;
        }

        private boolean didBeginRead = false;
        private boolean isReaderDone = false;

        public void fill(SourceBuilder.TimestampedSourceBuffer<T> buf) throws Exception {
            BufferedReader reader = getReader();

            long startTime = System.currentTimeMillis();

            while (true) {

                if (reader == null) {
                    throw new IOException("Input source \""+filePath+"\"not found.");
                }

                if (!reader.ready()) {

                    if (didBeginRead) {
                        isReaderDone = true;
                    }

                    break;
                }

                String line = reader.readLine();

                if (line == null) {
                    // Will never get here because of isReady()
                    buf.close(); // Will throw on StreamSources despite being used in the Jet docs
                    close();
                    break;
                }

                didBeginRead = true;

                if (line.trim().length() == 0)
                    continue;

                T parsedObject = parser.parseString(line);

                if (parsedObject == null) {
                    throw new ParserDeserializationException(line,"<T extends AnyProudData>");
                }

                buf.add(parsedObject, parsedObject.arrival);
            }

            state.put("DONE", true);
        }

        void close() {
            try {
                BufferedReader reader = getReader();

                if (reader != null)
                    reader.close();

            } catch (IOException e) {
                throw ExceptionUtils.sneaky(e);
            }
        }
    }

}
