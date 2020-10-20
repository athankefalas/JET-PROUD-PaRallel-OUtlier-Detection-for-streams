package edu.auth.jetproud.datastructures.mtree.exceptions;

public class SplitNodeReplacementException extends Exception
{
    // A subclass of Throwable cannot be generic.  :-(
    // So, we have newNodes declared as Object[] instead of Node[].
    public Object newNodes[];

    public SplitNodeReplacementException(Object... newNodes) {
        this.newNodes = newNodes;
    }
}
