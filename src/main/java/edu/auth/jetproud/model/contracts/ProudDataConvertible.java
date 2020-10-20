package edu.auth.jetproud.model.contracts;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.utils.Lists;

import java.util.List;

public interface ProudDataConvertible
{
    int identifier();
    List<Double> coordinates();
    long arrivalTime();

    default AnyProudData toProudData() {
        return new AnyProudData(identifier(), Lists.copyOf(coordinates()), arrivalTime(), 0);
    }
}
