package bqstorageapi.core.reader;

import org.apache.avro.generic.GenericRecord;

@FunctionalInterface
public interface RecordHandler {
    boolean onRecord(GenericRecord rec);
}