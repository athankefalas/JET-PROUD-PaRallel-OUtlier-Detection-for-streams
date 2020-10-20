package edu.auth.jetproud.application.parameters;

import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class ProudParameter<T> {

    // Cases
    public static ProudParameter<ProudSpaceOption> Space
            = make("Space", "space", ProudSpaceOption.parser());
    public static ProudParameter<ProudAlgorithmOption> Algorithm
            = make("Algorithm", "algorithm", ProudAlgorithmOption.parser());

    public static ProudParameter<List<Integer>> WindowSize
            = make("Window Size", "W", Parser.ofIntList(";"));

    public static ProudParameter<List<Integer>> SlideSize
            = make("Window Slide Size", "S", Parser.ofIntList(";"));

    public static ProudParameter<List<Integer>> KMinNeighbours
            = make("k Minimum Neighbours", "k", Parser.ofIntList(";"));

    public static ProudParameter<List<Double>> RNeighbourhoodRadius
            = make("Neighbourhood Radius", "R", Parser.ofDoubleList(";"));

    public static ProudParameter<String> Dataset
            = make("Dataset", "dataset", Parser.ofString());

    public static ProudParameter<ProudPartitioningOption> Partitioning
            = make("Partitioning", "partitioning", ProudPartitioningOption.parser());

    public static ProudParameter<Integer> TreeInitCount
            = make("Tree Initial Point Count", "tree_init", Parser.ofInt());

    public static ProudParameter<Boolean> Debug
            = make("Debug", "debug", Parser.ofBoolean());

    // Enum Conformance
    public static ProudParameter valueOf(String name) {
        Class<ProudParameter> klass = ProudParameter.class;

        for(Field field: klass.getFields()) {
            if (field.getName().equals(name)) {
                try {
                    Object value = field.get(null);

                    if (value instanceof ProudParameter) {
                        return (ProudParameter) value;
                    }

                } catch (Exception e) {
                    continue;
                }
            }
        }
        for(ProudParameter item:values()) {
            if (item.name.equals(name))
                return item;
        }

        return null;
    }

    public static ProudParameter[] values() {
        return new ProudParameter[] {
                Space,
                Algorithm,
                WindowSize,
                SlideSize,
                KMinNeighbours,
                RNeighbourhoodRadius,
                Dataset,
                Partitioning,
                TreeInitCount,
                Debug
        };
    }

    private String name;
    private String switchValue;
    private Parser<T> parser;

    private ProudParameter(String name, String switchValue, Parser<T> parser) {
        this.name = name;
        this.parser = parser;

        if (!switchValue.startsWith("--")) {
            this.switchValue = "--"+switchValue;
        } else if(!switchValue.startsWith("-")) {
            this.switchValue = "-"+switchValue;
        } else {
            this.switchValue = switchValue;
        }
    }

    public String getName() {
        return name;
    }

    public String getSwitchValue() {
        return switchValue;
    }

    public Parser<T> getParser() {
        return parser;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProudParameter<?> that = (ProudParameter<?>) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(switchValue);
    }

    @Override
    public String toString() {
        return "[ "+switchValue+" ("+name+") ]";
    }

    public T readValue(String argument) {
        return parser.parseString(argument);
    }

    // Static Factory and util Methods

    private static <T> ProudParameter<T> make(String name, String switchValue, Parser<T> parser) {
        return new ProudParameter<>(name, switchValue, parser);
    }

    public static ProudParameter forSwitch(String switchValue) {
        return Arrays.stream(values())
                .filter((it)->it.switchValue.equals(switchValue))
                .findFirst()
                .orElse(null);
    }

    public static List<ProudParameter> optionalParameters() {
        return Lists.of(TreeInitCount, Debug);
    }

    public static List<ProudParameter> requiredParameters() {
        List<ProudParameter> all = Lists.from(values());
        all.removeAll(optionalParameters());

        return all;
    }

}
