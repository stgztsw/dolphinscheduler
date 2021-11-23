package org.apache.dolphinscheduler.api.exceptions;

public class ProcessDependentSqlException extends RuntimeException{

    public ProcessDependentSqlException(String message) {
        super(message);
    }
}
