package edu.auth.jetproud.application.config;

import java.io.Serializable;

public class InfluxDBConfiguration implements Serializable
{
    public static final String INFLUXDB_HOST_PROPERTY = "INFLUXDB_HOST";
    public static final String INFLUXDB_USER_PROPERTY = "INFLUXDB_USER";
    public static final String INFLUXDB_PASSWORD_PROPERTY = "INFLUXDB_PASSWORD";
    public static final String INFLUXDB_DB_PROPERTY = "INFLUXDB_DB";

    private String host;
    private String user;
    private String password;
    private String database;

    private InfluxDBConfiguration() {
        this("", "", "", "");
    }

    public InfluxDBConfiguration(String host, String user, String password, String database) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public static InfluxDBConfiguration fromSystemEnvironment() {
        InfluxDBConfiguration config = new InfluxDBConfiguration();
        config.host = System.getenv(INFLUXDB_HOST_PROPERTY);
        config.user = System.getenv(INFLUXDB_USER_PROPERTY);
        config.password = System.getenv(INFLUXDB_PASSWORD_PROPERTY);
        config.database = System.getenv(INFLUXDB_DB_PROPERTY);

        return config;
    }

    public static InfluxDBConfiguration fromSystemProperties() {
        InfluxDBConfiguration config = new InfluxDBConfiguration();
        config.host = System.getProperty(INFLUXDB_HOST_PROPERTY);
        config.user = System.getProperty(INFLUXDB_USER_PROPERTY);
        config.password = System.getProperty(INFLUXDB_PASSWORD_PROPERTY);
        config.database = System.getProperty(INFLUXDB_DB_PROPERTY);

        return config;
    }
}
