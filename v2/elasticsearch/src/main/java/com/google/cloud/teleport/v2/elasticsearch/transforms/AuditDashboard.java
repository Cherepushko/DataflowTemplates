package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.elasticsearch.utils.CloudId;
import java.io.IOException;
import java.util.Iterator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Utility class for create Audit Dashboard in Kibana.
 **/
public class AuditDashboard {

    private static CloudId cloudId;
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void createDashboard(String cloudIdOption) throws IOException {

        cloudId = new CloudId(cloudIdOption);

        RestClientBuilder clientBuilder = RestClient.builder(new Node
                (new HttpHost(cloudId.getKibanaHost(),
                        9243,
                        "HTTPS")));

        RestClient client = clientBuilder.setDefaultHeaders(new Header[]{
                new BasicHeader("Authorization", "Basic AUTH"),
                new BasicHeader("kbn-xsrf", "true")
        }).build();

        sendDashboardCreationRequest(client, getGCPVersion(client));
    }

    private static String getGCPVersion(RestClient client) throws IOException {
        Response response = client.performRequest(
                new Request("GET", "/api/fleet/epm/packages?experimental=true"));
        HttpEntity responseEntity = new BufferedHttpEntity(response.getEntity());
        JsonNode node = mapper.readTree(responseEntity.getContent());

        JsonNode array = node.path("response");
        Iterator<JsonNode> elementsIterator = array.elements();

        while (elementsIterator.hasNext()) {
            JsonNode element = elementsIterator.next();
            if (element.path("name").asText().equals("gcp")) {
                return element.path("version").asText();
            }
        }

        throw new IOException("Error parsing GCP version.");
    }

    private static void sendDashboardCreationRequest(RestClient client, String version) throws IOException {
        Response response = client.performRequest(new Request("POST",
                "/api/fleet/epm/packages/gcp-" + version));
        HttpEntity responseEntity = new BufferedHttpEntity(response.getEntity());
        JsonNode node = mapper.readTree(responseEntity.getContent());
    }

}
