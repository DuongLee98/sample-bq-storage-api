package bqstorageapi.core.reader;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Helper nhỏ để decode một chunk AVRO từ ReadRowsResponse. */
public final class AvroUtils {
    private AvroUtils() {}

    public static List<GenericRecord> decodeChunk(ReadRowsResponse resp, Schema schema) throws IOException {
        long rc = resp.getRowCount();
        byte[] payload = resp.getAvroRows().getSerializedBinaryRows().toByteArray();
        Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        List<GenericRecord> out = new ArrayList<>();
        for (long i = 0; i < rc; i++) {
            out.add(reader.read(null, decoder));
        }
        return out;
    }
}