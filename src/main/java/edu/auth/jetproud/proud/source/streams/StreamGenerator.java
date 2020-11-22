package edu.auth.jetproud.proud.source.streams;

import edu.auth.jetproud.model.AnyProudData;

import java.io.Serializable;
import java.util.List;

public interface StreamGenerator extends Serializable
{
    boolean isComplete();

    List<AnyProudData> availableItems();

    void close();
}
