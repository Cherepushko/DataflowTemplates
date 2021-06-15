package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * The {@link PubSubToElasticsearchOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 *
 * <p>Inherits standard configuration options, options from {@link
 * JavascriptTextTransformer.JavascriptTextTransformerOptions}, and options from {@link ElasticsearchOptions}.
 */
public interface PubSubToElasticsearchOptions
        extends JavascriptTextTransformer.JavascriptTextTransformerOptions, ElasticsearchOptions {

    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description(
            "The output dead-letter topic to output to.")
    String getOutputDeadletterTopic();

    void setOutputDeadletterTopic(String outputDeadletterTopic);

    @Description("Define using Data Stream to use op_type CREATE instead of INDEX")
    @Default.Boolean(true)
    Boolean getUseDataStream();

    void setUseDataStream(Boolean useDataStream);
}
