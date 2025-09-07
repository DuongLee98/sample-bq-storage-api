package bqstorageapi.core.retry;

import bqstorageapi.config.RetryConfig;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;

import java.util.EnumSet;

/** Xác định lỗi có nên retry hay không dựa vào RetryConfig. (dùng sau) */
public final class RetryClassifier {
    private RetryClassifier() {}

    public static boolean isRetryable(Throwable t, EnumSet<RetryConfig.Code> allowed) {
        if (!(t instanceof ApiException)) return false;
        StatusCode.Code c = ((ApiException) t).getStatusCode().getCode();
        switch (c) {
            case UNAVAILABLE:           return allowed.contains(RetryConfig.Code.UNAVAILABLE);
            case DEADLINE_EXCEEDED:     return allowed.contains(RetryConfig.Code.DEADLINE_EXCEEDED);
            case ABORTED:               return allowed.contains(RetryConfig.Code.ABORTED);
            case INTERNAL:              return allowed.contains(RetryConfig.Code.INTERNAL);
            case RESOURCE_EXHAUSTED:    return allowed.contains(RetryConfig.Code.RESOURCE_EXHAUSTED);
            default: return false;
        }
    }
}