/*
 * This source file was generated by the Gradle 'init' task
 */
package com.antoniocali;

import com.antoniocali.bq.BigQueryReadOptions;
import com.antoniocali.sources.BigQueryDataStreamSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.logging.Logger;

public class Library {
    public static void main(String[] args) throws IOException {
        Logger LOGGER = Logger.getLogger(Library.class.getName());

        BigQueryReadOptions bigQueryReadOptions = BigQueryReadOptions.builder()
                                                          .setProjectId("data-mdm-dev")
                                                          .setDataset("dsp_data_ingestion_data_streaming_platform")
                                                          .setTable("dbt_full_test_2")
                                                          .setColumnFetcher("dbt_updated_at")
                                                          .setColumnFetcherTimeUnit(
                                                                  BigQueryReadOptions.TimeUnit.DAY)
                                                          .build();

        BigQueryDataStreamSource source = new BigQueryDataStreamSource(bigQueryReadOptions);
        Configuration flinkConfig = new Configuration();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(60000);
        LOGGER.info("Starting streaming job");
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "big-query-source-flink").print();
        try {
            env.execute("BQ Source");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
