package edu.auth.jetproud.proud.context.buildercontracts;

public interface InfluxDBDatabaseProudConfigBuilder {
    InfluxDBHostProudConfigBuilder inDatabase(String dbName);
}
