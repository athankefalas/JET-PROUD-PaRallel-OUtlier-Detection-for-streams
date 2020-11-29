import com.hazelcast.jet.Job;
import common.DatasetOutliersTestSet;
import common.ListStreamOutlierCollector;
import common.ResourceFiles;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.proud.source.ProudSource;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.concurrent.CompletableFuture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Stock Tests")
public class StockTests
{
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
            DatasetOutliersTestSet testSet = new DatasetOutliersTestSet("stk/stk_outliers.csv");
        }, "Stocks test dataset found but not loaded.");
    }

    @Order(3)
    @Test
    @DisplayName("Stocks - Naive")
    public void stocksNaive() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf("stk");
        DatasetOutliersTestSet testSet = new DatasetOutliersTestSet("stk/stk_outliers.csv");

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.AdvancedExtended)
                .inSingleSpace()
                .querying(100,1.0,1,1)
                .forDatasetNamed("stk")
                .locatedIn(datasetAbsolutePath)
                .treePartitionedUsing("stk_tree_input.txt")
                .printingOutliers()
                .enablingDebug()
                .build();

        ListStreamOutlierCollector collector = new ListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, "stk_input_300k.txt"))
                .partition()
                .detectOutliers()
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        while (!future.isDone())
            Thread.sleep(1000);

        collector.items.forEach(System.out::println);
    }




}
