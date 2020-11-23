package edu.auth.jetproud.application.config;

public class DatasetConfiguration
{
    public static final String DATASET_HOME_PROPERTY = "JOB_INPUT";

    private String datasetHome;

    private DatasetConfiguration() {
        this("");
    }

    public DatasetConfiguration(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public String getDatasetHome() {
        return datasetHome;
    }

    public void setDatasetHome(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public static DatasetConfiguration fromSystemEnvironment() {
        DatasetConfiguration config = new DatasetConfiguration();
        config.datasetHome = System.getenv(DATASET_HOME_PROPERTY);

        return config;
    }

    public static DatasetConfiguration fromSystemProperties() {
        DatasetConfiguration config = new DatasetConfiguration();
        config.datasetHome = System.getProperty(DATASET_HOME_PROPERTY);

        return config;
    }
}
