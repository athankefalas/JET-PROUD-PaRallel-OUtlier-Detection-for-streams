package edu.auth.jetproud.application.config;

public class ProudDatasetConfiguration
{
    public static final String DATASET_HOME_PROPERTY = "JOB_INPUT";

    private String datasetHome;

    private ProudDatasetConfiguration() {
        this("");
    }

    public ProudDatasetConfiguration(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public String getDatasetHome() {
        return datasetHome;
    }

    public void setDatasetHome(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public static ProudDatasetConfiguration fromSystemEnvironment() {
        ProudDatasetConfiguration config = new ProudDatasetConfiguration();
        config.datasetHome = System.getenv(DATASET_HOME_PROPERTY);

        return config;
    }

    public static ProudDatasetConfiguration fromSystemProperties() {
        ProudDatasetConfiguration config = new ProudDatasetConfiguration();
        config.datasetHome = System.getProperty(DATASET_HOME_PROPERTY);

        return config;
    }
}
