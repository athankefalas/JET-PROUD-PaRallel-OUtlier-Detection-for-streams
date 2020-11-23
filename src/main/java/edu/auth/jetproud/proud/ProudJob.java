package edu.auth.jetproud.proud;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.state.ProudStatistics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public class ProudJob implements Job
{
    public static class Statistics {
        private long totalSlideCount;
        private long totalCPUTime;
        private double averageTimePerSlide;

        private Statistics() {
            this(0,0,0);
        }

        public Statistics(long totalSlideCount, long totalCPUTime) {
            this.totalSlideCount = totalSlideCount;
            this.totalCPUTime = totalCPUTime;
            this.averageTimePerSlide = 0.0;

            if (totalSlideCount != 0 && totalCPUTime != 0) {
                double totalCPUTimeFP = 0.0 + totalCPUTime;
                double totalSlideCountFP = 0.0 + totalSlideCount;

                this.averageTimePerSlide = totalCPUTimeFP / totalSlideCountFP;
            }
        }

        private Statistics(long totalSlideCount, long totalCPUTime, double averageTimePerSlide) {
            this.totalSlideCount = totalSlideCount;
            this.totalCPUTime = totalCPUTime;
            this.averageTimePerSlide = averageTimePerSlide;
        }

        public long getTotalSlideCount() {
            return totalSlideCount;
        }

        private void setTotalSlideCount(long totalSlideCount) {
            this.totalSlideCount = totalSlideCount;
        }

        public long getTotalCPUTime() {
            return totalCPUTime;
        }

        private void setTotalCPUTime(long totalCPUTime) {
            this.totalCPUTime = totalCPUTime;
        }

        public double getAverageTimePerSlide() {
            return averageTimePerSlide;
        }

        private void setAverageTimePerSlide(double averageTimePerSlide) {
            this.averageTimePerSlide = averageTimePerSlide;
        }
    }

    private final ProudContext proudContext;

    private JetInstance jet;
    private Job job;

    private CompletableFuture<Void> completableFuture;

    private final boolean isDebugOn;

    private Long slideCount = 0L;
    private Long CPUTime = 0L;

    public ProudJob(ProudContext proudContext, JetInstance jet, Job job) {
        this.proudContext = proudContext;
        this.jet = jet;
        this.job = job;

        this.isDebugOn = proudContext.configuration().getDebug();
    }

    public ProudContext getProudContext() {
        return proudContext;
    }

    public JetInstance getJet() {
        return jet;
    }

    public Statistics proudStatistics() {
        return new Statistics(slideCount, CPUTime);
    }

    // Job Proxy

    @Override
    public long getId() {
        return job.getId();
    }

    @Override
    @Nonnull
    public String getIdString() {
        return job.getIdString();
    }

    @Override
    @Nonnull
    public JobConfig getConfig() {
        return job.getConfig();
    }

    @Override
    @Nullable
    public String getName() {
        return job.getName();
    }

    @Override
    public long getSubmissionTime() {
        return job.getSubmissionTime();
    }

    @Override
    @Nonnull
    public JobStatus getStatus() {
        return job.getStatus();
    }

    @Override
    @Nonnull
    public JobMetrics getMetrics() {
        return job.getMetrics();
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> getFuture() {
        join();
        return completableFuture;
    }

    @Override
    public void join() {
        job.join();
        completableFuture = job.getFuture().whenComplete((_void, err)->{
            slideCount = ProudStatistics.slideCounter().get();
            CPUTime = ProudStatistics.cpuTimeCounter().get();

            if (isDebugOn) {
                Statistics stats = proudStatistics();

                // Get and use jet logger ???
                System.out.println(
                        "Proud Job Completed. Statistics:\n" +
                        "Total window slides: "+stats.totalSlideCount+"\n" +
                        "Total cpu time (millis): "+stats.totalCPUTime+"\n" +
                        "Average time per slide (millis): "+stats.averageTimePerSlide+"\n"
                );
            }
        });
    }

    @Override
    public void restart() {
        job.restart();
    }

    @Override
    public void suspend() {
        job.suspend();
    }

    @Override
    public void resume() {
        job.resume();
    }

    @Override
    public void cancel() {
        job.cancel();
    }

    @Override
    public JobStateSnapshot cancelAndExportSnapshot(String name) {
        return job.cancelAndExportSnapshot(name);
    }

    @Override
    public JobStateSnapshot exportSnapshot(String name) {
        return job.exportSnapshot(name);
    }
}
