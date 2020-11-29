import com.hazelcast.jet.Job;
import common.DatasetOutliersTestSet;
import common.UnsafeListStreamOutlierCollector;
import common.ResourceFiles;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.ProudExecutor;
import edu.auth.jetproud.proud.context.Proud;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.pipeline.ProudPipeline;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.Triple;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
        //10000,500,0.45,50 (w|s|r|k)
        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Naive)
                .inSingleSpace()
                .querying(50,0.45,10000,500)
                .forDatasetNamed("stk")
                .locatedIn(datasetAbsolutePath)
                .replicationPartitioned()//treePartitionedUsing("stk_tree_input.txt")
                .printingOutliers()
                .enablingDebug()
                .build();

        UnsafeListStreamOutlierCollector collector = new UnsafeListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, "stk_input_300k.txt"))
                .partition()
                .detectOutliers()
                //.writeTo(Sinks.logger());
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        DistributedMap<String, Boolean> state = new DistributedMap<>("SOURCE_STATE");

        while (!future.isDone()) {
            //Thread.sleep(1000);

//            if (!state.getOrDefault("DONE", false))
//                continue; //else break;

        }

        List<Triple<Long, OutlierQuery, Long>> outliers = collector.items().stream()
                .sorted(Comparator.comparingLong(Triple::getFirst))
                .collect(Collectors.toList());

        outliers.forEach(System.out::println);
        System.out.println("[*]:\tDONE");
    }

    //@Order(4)
    //@Test
    @DisplayName("Stocks - Advanced")
    public void stocksAdvanced() throws Exception {
        String datasetAbsolutePath = ResourceFiles.absolutePathOf("stk");
        DatasetOutliersTestSet testSet = new DatasetOutliersTestSet("stk/stk_outliers.csv");

        Proud proud = Proud.builder()
                .forAlgorithm(ProudAlgorithmOption.Advanced)
                .inSingleSpace()
                .querying(100,1.0,1,1)
                .forDatasetNamed("stk")
                .locatedIn(datasetAbsolutePath)
                .replicationPartitioned()//treePartitionedUsing("stk_tree_input.txt")
                .printingOutliers()
                .enablingDebug()
                .build();

        UnsafeListStreamOutlierCollector collector = new UnsafeListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, "stk_input_300k.txt"))
                .partition()
                .detectOutliers()
                //.writeTo(ProudSink.logger(proud));
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        while (!future.isDone()) {
            Thread.sleep(1000);
        }

        collector.items().forEach(System.out::println);
    }

    @Order(5)
    @Test
    @DisplayName("Stocks - Advanced Extended")
    public void stocksAdvancedExt() throws Exception {
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

        UnsafeListStreamOutlierCollector collector = new UnsafeListStreamOutlierCollector();
        ProudPipeline pipeline = ProudPipeline.create(proud);

        pipeline.readFrom(ProudSource.file(proud, "stk_input_300k.txt"))
                .partition()
                .detectOutliers()
                //.writeTo(ProudSink.logger(proud));
                .writeTo(ProudSink.collector(proud, collector));

        Job job = ProudExecutor.executeJob(pipeline);
        CompletableFuture<Void> future = job.getFuture();

        while (!future.isDone()) {
            Thread.sleep(1000);
        }

        collector.items().forEach(System.out::println);
    }




}
