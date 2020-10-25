package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.exceptions.ParserDeserializationException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ProudFileSource<T extends AnyProudData> implements ProudSource<T> {
    private ProudContext proudContext;
    private Parser<T> parser;

    public ProudFileSource(ProudContext proudContext, Parser<T> parser) {
        this.proudContext = proudContext;
        this.parser = parser;
    }

    private FileBufferedReaderContext fileBufferedReaderContext(Processor.Context ctx) throws FileNotFoundException {
        String fileInput = proudContext.getProudDatasetResourceConfiguration().getDatasetHome();
        String dataset = proudContext.getProudConfiguration().getDataset();

        Path path = Paths.get(fileInput, dataset, "input_20k.txt");
        InputStreamReader inputStreamReader = new FileReader(path.toFile());
        BufferedReader reader = new BufferedReader(inputStreamReader);

        return new FileBufferedReaderContext(reader);
    }

    private void bufferFiller(FileBufferedReaderContext ctx, SourceBuilder.TimestampedSourceBuffer<T> buf) throws IOException, ProudException {
        BufferedReader reader = ctx.reader;

        while (true) {

            if (!reader.ready())
                break;

            String line = reader.readLine();

            if (line == null) {
                buf.close();
                break;
            }

            T parsedObject = parser.parseString(line);

            if (parsedObject == null) {
                throw new ParserDeserializationException(line,"<T extends AnyProudData>");
            }

            buf.add(parsedObject, parsedObject.arrival);
        }
    }

    @Override
    public StreamSource<T> createJetSource() {
        return SourceBuilder.timestampedStream("proud-source", this::fileBufferedReaderContext)
                .fillBufferFn(this::bufferFiller)
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

    private static class FileBufferedReaderContext {
        private final BufferedReader reader;

        FileBufferedReaderContext(BufferedReader reader) {
            this.reader = reader;
        }

        void close() {
            try {
                reader.close();
            } catch (IOException e) {
                throw ExceptionUtils.sneaky(e);
            }
        }
    }

}
