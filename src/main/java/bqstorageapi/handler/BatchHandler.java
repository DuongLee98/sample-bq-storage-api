package bqstorageapi.handler;

import org.apache.avro.generic.GenericRecord;
import java.util.List;

@FunctionalInterface
public interface BatchHandler {
    /**
     * Xử lý một batch. Trả về true nếu tiếp tục, false để dừng sớm.
     * @param batch       danh sách record trong batch (không rỗng)
     * @param approxBytes tổng bytes ước lượng của batch (thập phân)
     */
    boolean onBatch(List<GenericRecord> batch, long approxBytes) throws Exception;
}