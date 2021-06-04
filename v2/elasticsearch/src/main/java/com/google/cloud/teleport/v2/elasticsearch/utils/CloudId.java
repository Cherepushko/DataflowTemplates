package com.google.cloud.teleport.v2.elasticsearch.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * CloudId.
**/
public class CloudId {
    private String elasticsearchURL;
    private String kibanaURL;

    public String getElasticsearchURL() {
        return elasticsearchURL;
    }

    public void setElasticsearchURL(String elasticsearchURL) {
        this.elasticsearchURL = elasticsearchURL;
    }

    public String getKibanaURL() {
        return kibanaURL;
    }

    public void setKibanaURL(String kibanaURL) {
        this.kibanaURL = kibanaURL;
    }

    public CloudId(String cloudId){
        // there is an optional first portion of the cloudId that is a human readable string, but it is not used.
        if (cloudId.contains(":")) {
            if (cloudId.indexOf(":") == cloudId.length() - 1) {
                throw new IllegalStateException("cloudId " + cloudId + " must begin with a human readable identifier followed by a colon");
            }
            cloudId = cloudId.substring(cloudId.indexOf(":") + 1);
        }

        String decoded = new String(Base64.getDecoder().decode(cloudId), StandardCharsets.UTF_8);
        // once decoded the parts are separated by a $ character.
        // they are respectively domain name and optional port, elasticsearch id, kibana id
        String[] decodedParts = decoded.split("\\$");
        if (decodedParts.length != 3) {
            throw new IllegalStateException("cloudId " + cloudId + " did not decode to a cluster identifier correctly");
        }

        // domain name and optional port
        String[] domainAndMaybePort = decodedParts[0].split(":", 2);
        String domain = domainAndMaybePort[0];
        int port;

        if (domainAndMaybePort.length == 2) {
            try {
                port = Integer.parseInt(domainAndMaybePort[1]);
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("cloudId " + cloudId + " does not contain a valid port number");
            }
        } else {
            port = 443;
        }

        this.elasticsearchURL = "https://" + decodedParts[1]  + "." + domain + ":" + port;
        this.kibanaURL = "https://" + decodedParts[2]  + "." + domain + ":" + port;
    }
}
