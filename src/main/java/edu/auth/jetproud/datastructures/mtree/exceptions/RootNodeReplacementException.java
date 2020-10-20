package edu.auth.jetproud.datastructures.mtree.exceptions;

public class RootNodeReplacementException extends Exception
{
    // A subclass of Throwable cannot be generic.  :-(
    // So, we have newRoot declared as Object instead of Node.
    public Object newRoot;

    public RootNodeReplacementException(Object newRoot) {
        this.newRoot = newRoot;
    }
}
