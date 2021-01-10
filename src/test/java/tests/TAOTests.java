package tests;

import com.hazelcast.jet.Job;
import common.DatasetOutliersTestSet;
import common.ResourceFiles;
import common.UnsafeListStreamOutlierCollector;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.proud.partitioning.gridresolvers.DefaultGridPartitioners;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Triple;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("TAO Tests")
public class TAOTests
{

    private GridPartitioning.GridPartitioner gridPartitioner() {
        return DefaultGridPartitioners.forDatasetNamed("TAO");
    }

    private String dataSetName() {
        return "tao";
    }

    private String dataSetFolder() {
        return "tao";
    }

    private String dataSetFile() {
        return "tao_input_300k.txt";
    }

    private String dataTreeInitName() {
        return "tao_tree_input.txt";
    }

    private String testSetPath() {
        return "tao/tao_outliers.csv";
    }

    private void testExecuteProud(ProudContext proud) throws Exception  {
        DatasetOutliersTestSet testSet = new DatasetOutliersTestSet(testSetPath());

        UnsafeListStreamOutlierCollector collector = new UnsafeListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, dataSetFile()))
                .partition()
                .detectOutliers()
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        long startTime = System.currentTimeMillis();
        long jobTimeout = 80 * 60 * 60 * 1000; // About 6 minutes

        while (!future.isDone() && Math.abs(System.currentTimeMillis() - startTime) <= jobTimeout) {
            Thread.sleep(10000);
        }

        List<Triple<Long, OutlierQuery, Long>> outliers = collector.items().stream()
                .sorted(Comparator.comparingLong(Triple::getFirst))
                .filter((it)->it.first >= 0 && it.first <= 299000)
                .collect(Collectors.toList());

        Assertions.assertTrue(outliers.size() > 0, "No outliers detected");

        final boolean assertExactMatches = true;

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

        if (!future.isDone())
            job.cancel();
    }

    //////////// Dataset files exist

    @Order(1)
    @Test
    @DisplayName("TAO Dataset Exists")
    public void datasetExists() throws Exception {
        File taoDataset = ResourceFiles.fromPath("stk/stk_input_300k.txt");
        File taoInitDataset = ResourceFiles.fromPath("stk/stk_tree_input.txt");

        Assertions.assertNotNull(taoDataset, "TAO dataset not found.");
        Assertions.assertNotNull(taoInitDataset, "Tree init file for TAO dataset not found.");
    }

    @Order(2)
    @Test
    @DisplayName("TAO Test Dataset Exists")
    public void testDatasetExists() throws Exception {
        File taoTestDataset = ResourceFiles.fromPath("stk/stk_outliers.csv");
        Assertions.assertNotNull(taoTestDataset, "TAO test dataset not found.");

        Assertions.assertDoesNotThrow(()->{
            DatasetOutliersTestSet testSet = new DatasetOutliersTestSet(testSetPath());
        }, "TAO test dataset found but not loaded.");
    }

    //////////////// Single Query Space

    //// Naive

    @Order(3)
    @Test
    @DisplayName("TAO: Naive - Repl") // Last passed on: X
    public void taoNaive() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        //10000,500,1.9,50 (w|s|r|k)
        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: Advanced - Repl") // Last passed on: X
    public void taoAdvanced() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Advanced)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: Advanced Extended - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void taoAdvancedExt_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AdvancedExtended)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: Advanced Extended - Grid") // Last passed on: 6/12/2020 ~ first 14 slides
    public void taoAdvancedExt_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AdvancedExtended)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: Slicing - Tree") // Last passed on: X
    public void taoSlicing_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Slicing)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: Slicing - Grid") // Last passed on: X
    public void taoSlicing_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Slicing)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: PMCOD - Tree") // Last passed on: X
    public void taoPMCOD_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: PMCOD - Grid") // Last passed on: X
    public void taoPMCOD_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCod)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: PMCOD Net - Tree") // Last passed on: X
    public void taoPMCODNet_Tree() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCodNet)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: PMCOD Net - Grid") // Last passed on: X
    public void taoPMCODNet_Grid() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCodNet)
                .inSingleSpace()
                .querying(50,1.9,10000,500)
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
    @DisplayName("TAO: AMCOD - Tree") // Last passed on: ~
    public void taoAMCOD_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: AMCOD - Grid") // Last passed on: ~
    public void taoAMCOD_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AMCod)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: SOP - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void taoSOP_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: SOP - Grid") // Last passed on: X
    public void taoSOP_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: PSOD - Tree") // Last passed on: 6/12/2020 ~ first 14 slides
    public void taoPSOD_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: PSOD - Grid") // Last passed on: NullPointerException @ PSODProudAlExecutor LN 513 IN addNeighbour
    public void taoPSOD_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: PMCSky - Tree") // Last passed on: ~
    public void taoPMCSky_Tree() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQuerySpace()
                .querying(Lists.of(50),Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: PMCSky - Grid") // Last passed on: IndexOBException PMCSkyProudAlgExec LN 714 IN deletePoint
    public void taoPMCSky_Grid() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQuerySpace()
                .querying(Lists.of(50), Lists.of(1.9),10000,500)
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
    @DisplayName("TAO: SOP - Tree - MW") // Last passed on: 6/12/2020 ~ first 14 slides
    public void taoSOP_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
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
    @DisplayName("TAO: SOP - Grid - MW") // Last passed on: X
    public void taoSOP_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Sop)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
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
    @DisplayName("TAO: PSOD - Tree - MW") // Last passed on: STUCK AFTER SLIDE 1
    public void taoPSOD_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
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
    @DisplayName("TAO: PSOD - Grid - MW") // Last passed on: NullPointerException PSODProudAlgExec LN 513 addNeighbour
    public void taoPSOD_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PSod)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
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
    @DisplayName("TAO: PMCSky - Tree - MW") // Last passed on: Index out of bounds EX @ PMCSktProudAlgExec LN 873 IN deleteSmallWindowPoint
    public void taoPMCSky_Tree_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
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
    @DisplayName("TAO: PMCSky - Grid - MW") // Last passed on: Index Out of Bounds EX @ LN 873
    public void taoPMCSky_Grid_MW() throws Exception { // TODO: Query space params may have to be different
        String datasetAbsolutePath = ResourceFiles.absolutePathOf(dataSetFolder());

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.PMCSky)
                .inMultiQueryMultiWindowSpace()
                .querying(Lists.of(50), Lists.of(1.9),Lists.of(10000),Lists.of(500))
                .forDatasetNamed(dataSetName())
                .locatedIn(datasetAbsolutePath)
                .gridPartitionedUsing(gridPartitioner())
                .printingOutliers()
                .enablingDebug()
                .build();

        testExecuteProud(proud);
    }



}
