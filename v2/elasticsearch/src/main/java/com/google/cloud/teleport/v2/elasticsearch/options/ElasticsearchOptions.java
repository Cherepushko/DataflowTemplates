package com.google.cloud.teleport.v2.elasticsearch.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link ElasticsearchOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 */

public interface ElasticsearchOptions extends PipelineOptions{

    @Description(
            "Specify application (BigQueryToElasticsearch, CsvToElasticsearch, PubSubToElasticsearch)")
    @Validation.Required
    String getApplication();

    void setApplication(String application);

    @Description(
            "Specify CloudID 'HOST$ES-ID$KB-ID'")
    @Validation.Required
    String getCloudID();

    void setCloudID(String application);

    @Description(
            "Specify ApiKey")
    @Validation.Required
    String getApiKey();

    void setApiKey(String apiKey);

    @Description(
            "Username for elasticsearch nodes")
    //@Validation.Required
    String getElasticsearchUsername();

    void setElasticsearchUsername(String elasticsearchUsername);

    @Description(
            "Password for elasticsearch nodes")
    //@Validation.Required
    String getElasticsearchPassword();

    void setElasticsearchPassword(String elasticsearchPassword);

   /* @Description(
            "Comma separated list of Elasticsearch nodes to connect to, ex: http://my-node1,http://my-node2")
    @Validation.Required
    String getNodeAddresses();

    void setNodeAddresses(String nodeAddresses);*/

    @Description("The index toward which the requests will be issued, ex: my-index")
    @Validation.Required
    String getIndex();

    void setIndex(String index);

    @Description("The document type toward which the requests will be issued, ex: my-document-type")
    @Default.String("_doc")
    String getDocumentType();

    void setDocumentType(String documentType);

    @Description("Batch size in number of documents. Default: 1000")
    @Default.Long(1000)
    Long getBatchSize();

    void setBatchSize(Long batchSize);

    @Description("Batch size in number of bytes. Default: 5242880 (5mb)")
    @Default.Long(5242880)
    Long getBatchSizeBytes();

    void setBatchSizeBytes(Long batchSizeBytes);

    @Description("Optional: Max retry attempts, must be > 0, ex: 3. Default: no retries")
    Integer getMaxRetryAttempts();

    void setMaxRetryAttempts(Integer maxRetryAttempts);

    @Description(
            "Optional: Max retry duration in milliseconds, must be > 0, ex: 5000L. Default: no retries")
    Long getMaxRetryDuration();

    void setMaxRetryDuration(Long maxRetryDuration);

    @Description("Set to true to issue partial updates. Default: false")
    @Default.Boolean(false)
    Boolean getUsePartialUpdate();

    void setUsePartialUpdate(Boolean usePartialUpdates);
}
