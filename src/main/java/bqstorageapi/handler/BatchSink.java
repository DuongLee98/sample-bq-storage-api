package bqstorageapi.handler;

import java.util.List;

@FunctionalInterface
public interface BatchSink<T> {
    /** Nên throws Exception để caller biết mà retry/stop */
    void accept(List<T> batch) throws Exception;
}