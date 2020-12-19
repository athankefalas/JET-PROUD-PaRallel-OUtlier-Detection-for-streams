package tests;

import com.hazelcast.jet.Job;
import common.DatasetOutliersTestSet;
import common.ResourceFiles;
import common.UnsafeListStreamOutlierCollector;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.proud.partitioning.gridresolvers.DefaultGridPartitioners;
import edu.auth.jetproud.proud.partitioning.gridresolvers.StockGridPartitioner;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.utils.Triple;
import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Stock Tests")
public class StockTests
{

    private GridPartitioning.GridPartitioner gridPartitioner() {
        return DefaultGridPartitioners.forDatasetNamed("STK");
    }

    private String dataSetName() {
        return "stk";
    }

    private String dataSetFolder() {
        return "stk";
    }

    private String dataSetFile() {
        return "stk_input_300k.txt";
    }

    private String dataTreeInitName() {
        return "stk_tree_input.txt";
    }

    private String testSetPath() {
        return "stk/stk_outliers.csv";
    }

    private void testExecuteProud(ProudContext proud) throws Exception {
        DatasetOutliersTestSet testSet = new DatasetOutliersTestSet(testSetPath());

        UnsafeListStreamOutlierCollector collector = new UnsafeListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, dataSetFile()))
                .partition()
                .detectOutliers()
                //.writeTo(Sinks.logger());
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        long startTime = System.currentTimeMillis();
        long jobTimeout = 1 * 60 * 1000; // About 6 minutes

        while (!future.isDone() && Math.abs(System.currentTimeMillis() - startTime) <= jobTimeout)
            continue;

        List<Triple<Long, OutlierQuery, Long>> outliers = collector.items().stream()
                .sorted(Comparator.comparingLong(Triple::getFirst))
                .filter((it)->it.first >= 0 && it.first <= 299000)
                .collect(Collectors.toList());

        Assertions.assertTrue(outliers.size() > 0, "No outliers detected");

        final boolean assertExactMatches = false;

        boolean allMatch = true;

        long falsePositives = 0;
        long falseNegatives = 0;

        for (Triple<Long, OutlierQuery, Long> outlier:outliers) {
            boolean matches = testSet.matches(outlier.first, outlier.third);

            if (matches) {
                System.out.println("PROUD: Outlier count for slide "+outlier.first+
                        " matched test dataset count of "+outlier.third+".");
            } else {
                long outlierCount = outlier.third;
                long trueOutlierCount = testSet.outlierCountOn(outlier.first);

                System.out.println("PROUD: Outlier count for slide "+outlier.first+
                        " did NOT match test dataset, expected "+trueOutlierCount+" got "+outlierCount+".");

                long absDelta = Math.abs(outlierCount - trueOutlierCount);

                if (trueOutlierCount > outlierCount) {
                    falseNegatives += absDelta;
                } else if(trueOutlierCount < outlierCount) {
                    falsePositives += absDelta;
                }
            }

            allMatch = allMatch & matches;

            if (!assertExactMatches)
                continue;

            String failureMessage = "Outlier count for slide "+outlier.first+
                    " was "+outlier.third+" instead of "+testSet.outlierCountOn(outlier.first);
            Assertions.assertTrue(matches, failureMessage);
        }

        if (allMatch) {
            System.out.println("PROUD: All slides match.");
        } else {
            System.out.println("PROUD: NOT All slides match.");
            System.out.println("Proud: False Positive Count= "+falsePositives);
            System.out.println("Proud: False Negative Count= "+falseNegatives);
        }

        job.cancel();
    }

    @Order(0)
    @Test
    @DisplayName("Stock Grid Partitioning")
    public void stockGridPartitioning() throws Exception { // fixes from: // https://datalab.csd.auth.gr/~gounaris/2019WI.pdf
        StockGridPartitioner gridPartitioner = new StockGridPartitioner();

        File in = ResourceFiles.fromPath("stk/stk_input_300k.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)));

        String line = "";
        List<String> lines = Lists.make();

        while(true) {
            line = br.readLine();

            if (line == null)
                break;

            lines.add(line);
        }

        List<AnyProudData> dataSet = lines.stream()
                .map((ln)->{
                    String[] parts = ln.split("&");

                    if (parts.length != 2)
                        return null;

                    Long id = Parser.ofLong().parseString(parts[0]);
                    Double value = Parser.ofDouble().parseString(parts[1]);

                    if (id == null || value == null)
                        return null;

                    return new AnyProudData(id.intValue(), Lists.of(value), id, 0);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        double minV = dataSet.stream()
                .map((it)->it.value.get(0))
                .mapToDouble((it)->it)
                .min()
                .orElse(Double.MIN_VALUE);

        double maxV = dataSet.stream()
                .map((it)->it.value.get(0))
                .mapToDouble((it)->it)
                .max()
                .orElse(Double.MAX_VALUE);

        double valueRange = maxV - minV;
        double partSize = valueRange / 16.0;

        double current = minV;
        List<Double> bounds = Lists.make();

        for (int n=1;n<16;n++) {
            current += partSize;
            bounds.add(current);
        }

        final DecimalFormat format = new DecimalFormat("#.##");
        String equalPartitionBoundsString = bounds.stream()
                .map((it)->format.format(it).replace(",", "."))
                .collect(Collectors.joining(";"));

        double range = 0.45;

        for (AnyProudData data: dataSet) {
            GridPartitioning.PartitionNeighbourhood old = gridPartitioner.originalNeighbourhoodOf(data, range);
            GridPartitioning.PartitionNeighbourhood mine = gridPartitioner.neighbourhoodOf(data, range);

            if (!old.toString().equals(mine.toString())) {
                System.out.println("Incorrect partitions.");
                mine = gridPartitioner.neighbourhoodOf(data, range);
            }
        }
    }

    //////////// Dataset files exist

    @Order(1)
    @Test
    @DisplayName("Stock Dataset Exists")
    public void datasetExists() throws Exception {
        File stockDataset = ResourceFiles.fromPath("stk/stk_input_300k.txt");
        File stockInitDataset = ResourceFiles.fromPath("stk/stk_tree_input.txt");

        Assertions.assertNotNull(stockDataset, "Stocks dataset not found.");
        Assertions.assertNotNull(stockInitDataset, "Tree init file for stocks dataset not found.");
    }

    @Order(2)
    @Test
    @DisplayName("Stock Test Dataset Exists")
    public void testDatasetExists() throws Exception {
        File stockTestDataset = ResourceFiles.fromPath("stk/stk_outliers.csv");
        Assertions.assertNotNull(stockTestDataset, "Stocks test dataset not found.");

        Assertions.assertDoesNotThrow(()->{
            DatasetOutliersTestSet testSet = new DatasetOutliersTestSet(testSetPath());
        }, "Stocks test dataset found but not loaded.");
    }

    //////////////// Single Query Space

    //// Naive

    @Order(3)
    @Test
    @DisplayName("Stocks: Naive - Repl") // Last passed on: X
    public void stocksNaive() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        //10000,500,0.45,50 (w|s|r|k)
        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .replicationPartitioned()
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    //// Advanced

    @Order(4)
    @Test
    @DisplayName("Stocks: Advanced - Repl") // Last passed on: X
    public void stocksAdvanced() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Advanced)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .replicationPartitioned()
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    //// Adv. Extended

    @Order(5)
    @Test
    @DisplayName("Stocks: Advanced Extended - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void stocksAdvancedExt_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AdvancedExtended)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(6)
    @Test
    @DisplayName("Stocks: Advanced Extended - Grid") // Last passed on: 6/12/2020 ~ first 14 slides
    public void stocksAdvancedExt_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AdvancedExtended)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    //// Slicing

    @Order(7)
    @Test
    @DisplayName("Stocks: Slicing - Tree") // Last passed on: X
    public void stocksSlicing_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Slicing)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(8)
    @Test
    @DisplayName("Stocks: Slicing - Grid") // Last passed on: X
    public void stocksSlicing_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Slicing)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    //// PMCOD

    @Order(9)
    @Test
    @DisplayName("Stocks: PMCOD - Tree") // Last passed on: X
    public void stocksPMCOD_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(10)
    @Test
    @DisplayName("Stocks: PMCOD - Grid") // Last passed on: X
    public void stocksPMCOD_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }


    //// PMCOD NET

    @Order(11)
    @Test
    @DisplayName("Stocks: PMCOD Net - Tree") // Last passed on: X
    public void stocksPMCODNet_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCodNet)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(12)
    @Test
    @DisplayName("Stocks: PMCOD Net - Grid") // Last passed on: X
    public void stocksPMCODNet_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCodNet)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }



    //////////////// Multi Space Query Space

    //// AMCOD

    @Order(13)
    @Test
    @DisplayName("Stocks: AMCOD - Tree") // Last passed on: ~
    public void stocksAMCOD_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(14)
    @Test
    @DisplayName("Stocks: AMCOD - Grid") // Last passed on: ~
    public void stocksAMCOD_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    //// SOP

    @Order(15)
    @Test
    @DisplayName("Stocks: SOP - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void stocksSOP_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(16)
    @Test
    @DisplayName("Stocks: SOP - Grid") // Last passed on: X
    public void stocksSOP_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }


    //// PSOD

    @Order(17)
    @Test
    @DisplayName("Stocks: PSOD - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void stocksPSOD_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(18)
    @Test
    @DisplayName("Stocks: PSOD - Grid") // Last passed on: NullPointerException @ PSODProudAlExecutor LN 513 IN addNeighbour
    public void stocksPSOD_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }


    //// PMCSky

    @Order(19)
    @Test
    @DisplayName("Stocks: PMCSky - Tree") // Last passed on: ~
    public void stocksPMCSky_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(20)
    @Test
    @DisplayName("Stocks: PMCSky - Grid") // Last passed on: IndexOBException PMCSkyProudAlgExec LN 714 IN deletePoint
    public void stocksPMCSky_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(0.45),10000,500)
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }



    //////////////// Multi Space Multi Window Query Space

    //// SOP

    @Order(21)
    @Test
    @DisplayName("Stocks: SOP - Tree - MW") // Last passed on: 6/12/2020 ~ first 14 slides
    public void stocksSOP_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(22)
    @Test
    @DisplayName("Stocks: SOP - Grid - MW") // Last passed on: X
    public void stocksSOP_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }


    //// PSOD

    @Order(23)
    @Test
    @DisplayName("Stocks: PSOD - Tree - MW") // Last passed on: STUCK AFTER SLIDE 1
    public void stocksPSOD_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(24)
    @Test
    @DisplayName("Stocks: PSOD - Grid - MW") // Last passed on: NullPointerException PSODProudAlgExec LN 513 addNeighbour
    public void stocksPSOD_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }


    //// PMCSky

    @Order(25)
    @Test
    @DisplayName("Stocks: PMCSky - Tree - MW") // Last passed on: Index out of bounds EX @ PMCSktProudAlgExec LN 873 IN deleteSmallWindowPoint
    public void stocksPMCSky_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing(dataTreeInitName())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }

    @Order(26)
    @Test
    @DisplayName("Stocks: PMCSky - Grid - MW") // Last passed on: Index Out of Bounds EX @ LN 873
    public void stocksPMCSky_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(0.45),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }



}
