package com.antoniocali.bq;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryTableInfo;
import lombok.Getter;
import lombok.NonNull;

import java.io.FileInputStream;
import java.io.IOException;

@Getter
public class BigQueryClient {


    private final BigQuery bigQuery;

    private final String credentialsPath = null;

    @NonNull
    private final BigQueryReadOptions readOptions;

    private BigQueryClient(BigQueryClientBuilder builder) throws IOException {
        this.readOptions = builder.readOptions;
        GoogleCredentials credentials;
        if (builder.credentialsPath != null) {
            try (FileInputStream credentialsStream = new FileInputStream(builder.credentialsPath)) {
                credentials = GoogleCredentials.fromStream(credentialsStream);
            }
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder()
                                                         .setCredentials(credentials)
                                                         .setProjectId(builder.readOptions.getProjectId());

        this.bigQuery = optionsBuilder.build().getService();
    }

    public TableSchema getTableSchema() {
        return BigQueryTableInfo.getSchema(this.bigQuery, this.readOptions.getProjectId(),
                this.readOptions.getDataset(), this.readOptions.getTable());
    }


    public static BigQueryClientBuilder builder() {
        return new BigQueryClientBuilder();
    }

    public static class BigQueryClientBuilder {
        private String credentialsPath;
        private BigQueryReadOptions readOptions;

        BigQueryClientBuilder() {
        }


        public BigQueryClientBuilder setCredentialsPath(String credentialsPath) {
            this.credentialsPath = credentialsPath;
            return this;
        }

        public BigQueryClientBuilder setReadOptions(BigQueryReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public BigQueryClient build() throws IOException {
            if (readOptions == null) {
                throw new IllegalStateException("Read options must be set");
            }
            return new BigQueryClient(this);
        }
    }

}