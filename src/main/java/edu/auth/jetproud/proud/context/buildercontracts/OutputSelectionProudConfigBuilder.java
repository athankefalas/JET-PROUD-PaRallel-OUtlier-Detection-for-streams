package edu.auth.jetproud.proud.context.buildercontracts;

public interface OutputSelectionProudConfigBuilder {
    DebugSelectionProudConfigBuilder printingOutliers();

    InfluxDBDatabaseProudConfigBuilder writingOutliersToInfluxDB();
}
