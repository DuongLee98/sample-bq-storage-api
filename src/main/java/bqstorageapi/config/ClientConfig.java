package bqstorageapi.config;

import java.nio.file.Path;
import java.util.Objects;

/** Cấu hình client: thông tin dự án, vùng, và credential. */
public final class ClientConfig {
    public final String projectId;
    public final String dataset;
    public final String location;          // ví dụ: "US", "asia-southeast1"
    public final Path serviceAccountJson;  // null => dùng ADC

    private ClientConfig(Builder b) {
        this.projectId = Objects.requireNonNull(b.projectId, "projectId");
        this.dataset = Objects.requireNonNull(b.dataset, "dataset");
        this.location = Objects.requireNonNull(b.location, "location");
        this.serviceAccountJson = b.serviceAccountJson;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private String projectId;
        private String dataset;
        private String location = "US";
        private Path serviceAccountJson; // nếu null => Application Default Credentials

        public Builder projectId(String v) { this.projectId = v; return this; }
        public Builder dataset(String v)   { this.dataset = v; return this; }
        public Builder location(String v)  { this.location = v; return this; }
        public Builder serviceAccountJson(Path p) { this.serviceAccountJson = p; return this; }

        public ClientConfig build() { return new ClientConfig(this); }
    }
}