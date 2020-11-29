package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.exceptions.ParserDeserializationException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private FileBufferedReaderContext<T> fileBufferedReaderContext(Processor.Context ctx) throws FileNotFoundException {
        String datasetHomeDir = proudContext.datasetConfiguration().getDatasetHome();
        String dataset = proudContext.configuration().getDataset();

        Path path = Paths.get(datasetHomeDir, fileName);
        return new FileBufferedReaderContext<>(path.toString(), parser);
    }

    @Override
    public StreamSource<T> createJetSource() {
        return SourceBuilder.timestampedStream("proud-source", this::fileBufferedReaderContext)
                .fillBufferFn(FileBufferedReaderContext<T>::fill)
                .destroyFn(FileBufferedReaderContext::close)
                .build();
    }

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
                    this.reader = null;
                }
            }

            return reader;
        }

        private boolean didBeginRead = false;
        private boolean isReaderDone = false;

        public void fill(SourceBuilder.TimestampedSourceBuffer<T> buf) throws Exception {
            BufferedReader reader = getReader();

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
