package edu.auth.jetproud.application.config;

public class ProudDatasetResourceConfiguration
{
    public static final String DATASET_HOME_PROPERTY = "JOB_INPUT";

    private String datasetHome;

    private ProudDatasetResourceConfiguration() {
        this("");
    }

    public ProudDatasetResourceConfiguration(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public String getDatasetHome() {
        return datasetHome;
    }

    public void setDatasetHome(String datasetHome) {
        this.datasetHome = datasetHome;
    }

    public static ProudDatasetResourceConfiguration fromSystemEnvironment() {
        ProudDatasetResourceConfiguration config = new ProudDatasetResourceConfiguration();
        config.datasetHome = System.getenv(DATASET_HOME_PROPERTY);

        return config;
    }

    public static ProudDatasetResourceConfiguration fromSystemProperties() {
        ProudDatasetResourceConfiguration config = new ProudDatasetResourceConfiguration();
        config.datasetHome = System.getProperty(DATASET_HOME_PROPERTY);

        return config;
    }
}
