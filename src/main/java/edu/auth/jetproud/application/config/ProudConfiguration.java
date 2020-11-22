package edu.auth.jetproud.application.config;

import edu.auth.jetproud.application.parameters.ProudParameterValue;
import edu.auth.jetproud.application.parameters.ProudParameter;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.proud.partitioning.gridresolvers.DefaultGridPartitioners;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class ProudConfiguration implements Serializable {

    public static final Integer defaultTreeInitialNodeCount = 10000;
    public static final Boolean defaultDebug = false;

    @ProudParameterValue(Switch = "--space")
    private ProudSpaceOption space;

    @ProudParameterValue(Switch = "--algorithm")
    private ProudAlgorithmOption algorithm;

    @ProudParameterValue(Switch = "--W")
    private List<Integer> windowSizes;

    @ProudParameterValue(Switch = "--S")
    private List<Integer> slideSizes;

    @ProudParameterValue(Switch = "--k")
    private List<Integer> kNeighbours;

    @ProudParameterValue(Switch = "--R")
    private List<Double> rNeighbourhood;

    @ProudParameterValue(Switch = "--dataset")
    private String dataset;

    @ProudParameterValue(Switch = "--partitioning")
    private ProudPartitioningOption partitioning;

    private GridPartitioning.GridPartitioner customGridPartitioner;

    @ProudParameterValue(Switch = "--tree_init")
    private Integer treeInitialNodeCount;

    @ProudParameterValue(Switch = "--debug")
    private Boolean debug;

    public ProudConfiguration() {
    }

    public ProudSpaceOption getSpace() {
        return space;
    }

    public void setSpace(ProudSpaceOption space) {
        this.space = space;
    }

    public ProudAlgorithmOption getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(ProudAlgorithmOption algorithm) {
        this.algorithm = algorithm;
    }

    public List<Integer> getWindowSizes() {
        return windowSizes;
    }

    public void setWindowSizes(List<Integer> windowSizes) {
        this.windowSizes = windowSizes;
    }

    public List<Integer> getSlideSizes() {
        return slideSizes;
    }

    public void setSlideSizes(List<Integer> slideSizes) {
        this.slideSizes = slideSizes;
    }

    public List<Integer> getKNeighbours() {
        return kNeighbours;
    }

    public void setKNeighbours(List<Integer> kNeighbours) {
        this.kNeighbours = kNeighbours;
    }

    public List<Double> getRNeighbourhood() {
        return rNeighbourhood;
    }

    public void setRNeighbourhood(List<Double> rNeighbourhood) {
        this.rNeighbourhood = rNeighbourhood;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public ProudPartitioningOption getPartitioning() {
        return partitioning;
    }

    public void setPartitioning(ProudPartitioningOption partitioning) {
        this.partitioning = partitioning;
    }

    public GridPartitioning.GridPartitioner getCustomGridPartitioner() {
        return customGridPartitioner;
    }

    public void setCustomGridPartitioner(GridPartitioning.GridPartitioner customGridPartitioner) {
        this.customGridPartitioner = customGridPartitioner;
    }

    public Integer getTreeInitialNodeCount() {
        return treeInitialNodeCount;
    }

    public void setTreeInitialNodeCount(Integer treeInitialNodeCount) {
        this.treeInitialNodeCount = treeInitialNodeCount;
    }

    public Boolean getDebug() {
        return debug;
    }

    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public void setAllFrom(Map<ProudParameter, Object> map) throws ProudArgumentException {

        // Use Reflection to set the values of each field from the parameter map
        for (ProudParameter parameter:map.keySet()) {
            Field matchingField = fieldForParameter(parameter);

            if (matchingField == null)
                throw ProudArgumentException.internalParseError("No configuration field matching '"+parameter+"' was found.");

            try {
                matchingField.set(this, map.get(parameter));
            } catch (IllegalAccessException e) {
                throw ProudArgumentException.internalParseError("Failed to set configuration field for parameter '"+parameter+"'.");
            }
        }

        // Set default values if needed

        if (partitioning == ProudPartitioningOption.Tree && !map.containsKey(ProudParameter.TreeInitCount)) {
            treeInitialNodeCount = defaultTreeInitialNodeCount;
        }

        if (!map.containsKey(ProudParameter.Debug)) {
            debug = defaultDebug;
        }
    }

    private Field fieldForParameter(ProudParameter parameter) {

        for (Field field:this.getClass().getDeclaredFields()) {
            ProudParameterValue proudParameterValueAnnotation = field.getAnnotation(ProudParameterValue.class);

            if (proudParameterValueAnnotation == null)
                continue;

            if (proudParameterValueAnnotation.Switch().equals(parameter.getSwitchValue()))
                return field;
        }

        return null;
    }

    public void validateConfiguration() throws ProudArgumentException {
        // Validate the consistency of the configuration values
        List<ProudAlgorithmOption> replicationAlgorithms = Lists.of(ProudAlgorithmOption.Naive, ProudAlgorithmOption.Advanced);

        if (replicationAlgorithms.contains(algorithm) && partitioning != ProudPartitioningOption.Replication) {
            // naive or advanced REQUIRE partitioning == Replication
            throw ProudArgumentException.invalid("Algorithm "+algorithm+" must use "+ProudPartitioningOption.Replication+" partitioning.");
        }

        if (!replicationAlgorithms.contains(algorithm) && partitioning == ProudPartitioningOption.Replication) {
            // !naive or !advanced REQUIRE partitioning != Replication
            throw ProudArgumentException.invalid("Algorithm "+algorithm+" must not use "+ProudPartitioningOption.Replication+" partitioning.");
        }

        if (partitioning != ProudPartitioningOption.Tree && treeInitialNodeCount != null) {
            // --tree_init REQUIRES partitioning == Tree
            throw ProudArgumentException.invalid("Initial tree node count can not be used with "+partitioning+" partitioning.");
        }

        // Grid partitioning requires a grid partitioning function
        // if a predefined one cannot be used throw an error
        if (partitioning == ProudPartitioningOption.Grid) {
            boolean canUseDefaultGridPartitioner = DefaultGridPartitioners.forDatasetNamed(dataset) != null;

            if (!canUseDefaultGridPartitioner && customGridPartitioner == null) {
                throw ProudArgumentException.missing("grid partitioning function", "configuration.customGridPartitioner");
            }
        }

    }

}
