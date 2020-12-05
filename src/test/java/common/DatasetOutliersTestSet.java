package common;

import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.utils.Triple;
import edu.auth.jetproud.utils.Lists;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DatasetOutliersTestSet
{

    private String csvFilePath;
    private List<Triple<Long, OutlierQuery, Long>> lines;

    public DatasetOutliersTestSet(String csvFilePath) throws IOException {
        this.csvFilePath = csvFilePath;
        this.lines = Lists.make();

        readLines();
    }

    //// IO - Dataset CSV parsing

    public void readLines() throws IOException {
        File file = ResourceFiles.fromPath(csvFilePath);

        if (file == null) {
            throw new IOException("File not found.");
        }

        InputStreamReader inputStreamReader = new FileReader(file);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        String line;
        Parser<Triple<Long, OutlierQuery, Long>> parser = lineParser();

        int count = 0;

        while (true) {
            line = reader.readLine();
            count++;

            if (line == null)
                break;

            Triple<Long, OutlierQuery, Long> parsed = parser.parseString(line);

            if (parsed == null || parsed.first == null || parsed.second == null || parsed.third == null) {
                if (count == 1) {// Skip Header line
                    continue;
                } else { // Unreadable line
                    throw new IOException("Unreadable file format.");
                }
            }

            lines.add(parsed);
        }
    }

    private Parser<Triple<Long, OutlierQuery, Long>> lineParser() {
        return (line)->{
            Triple<Long, OutlierQuery, Long> item = new Triple<>();
            List<String> parts = cutCSVLine(line);

            if (parts.size() != 3)
                return null;

            Parser<Long> longParser = Parser.ofLong();
            Parser<OutlierQuery> queryParser = outlierQueryParser();

            // Order
            // slide,query,outliers_count
            item.first = longParser.parseString(parts.get(0));
            item.second = queryParser.parseString(parts.get(1));
            item.third = longParser.parseString(parts.get(2));

            return item;
        };
    }

    private List<String> cutCSVLine(String line) {
        List<Integer> cutPoints = Lists.make();

        boolean isInsideString = false;
        for (int i=0;i<line.length();i++) {
            char current = line.charAt(i);

            if (current == '"')
                isInsideString = !isInsideString;

            if (current == ',' && !isInsideString)
                cutPoints.add(i);
        }

        cutPoints.add(line.length());

        // 1000,"(10000,500,0.45,50)",245
        List<String> parts = Lists.make();

        int start = 0;

        for (int cutPoint:cutPoints) {
            String part = line.substring(start, cutPoint);
            start = cutPoint + 1;

            parts.add(part);
        }

        return parts;
    }

    private Parser<OutlierQuery> outlierQueryParser() {
        return (str)-> {
            String cleaned = str.replace("\"", "")
                  .replace("(", "")
                  .replace(")", "")
                  .trim();

            String[] parts = cleaned.split(",");

            if (parts.length != 4)
              return null;

            Parser<Integer> intParser = Parser.ofInt();
            Parser<Double> doubleParser = Parser.ofDouble();

            OutlierQuery parsed = new OutlierQuery(0,0,0,0);

            // Order
            // W, S, R, k
            parsed.window = intParser.parseString(parts[0]);
            parsed.slide = intParser.parseString(parts[1]);
            parsed.range = doubleParser.parseString(parts[2]);
            parsed.kNeighbours = intParser.parseString(parts[3]);

            return parsed;
        };
    }

    //// API

    public int linesCount() {
        return lines.size();
    }

    public OutlierQuery usedParams() {
        Triple<Long, OutlierQuery, Long> line = lines.stream()
                .findFirst()
                .orElse(null);

        if (line == null)
            return null;

        return line.second.withOutlierCount(0);
    }

    public long slideAt(int index) {
        return lines.get(index).first;
    }

    public OutlierQuery queryAt(int index) {
        return lines.get(index).second;
    }

    public long outlierCountAt(int index) {
        return lines.get(index).third;
    }

    public long outlierCountOn(long slide) {
        return lines.stream()
                .filter((it)->it.first == slide)
                .map((it)->it.first)
                .findFirst()
                .orElse(0L);
    }

    public boolean matches(long slide, long outlierCount) {
        if (lines == null)
            return false;

        Triple<Long, OutlierQuery, Long> line = lines.stream()
                .filter((it)->it.first == slide)
                .findFirst()
                .orElse(null);

        if (line == null)
            return outlierCount == 0;

        return line.third == outlierCount;
    }


}
