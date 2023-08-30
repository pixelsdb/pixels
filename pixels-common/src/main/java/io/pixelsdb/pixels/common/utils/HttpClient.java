package io.pixelsdb.pixels.common.utils;
import org.asynchttpclient.*;

import java.io.IOException;

public class HttpClient {
    public static void main(String[] args) throws IOException {

        try (AsyncHttpClient httpClient = Dsl.asyncHttpClient()) {
            String serverIpAddress = "127.0.0.1";
            int serverPort = 8080;
            Request request = httpClient.prepareGet("http://" + serverIpAddress + ":" + serverPort + "/")
                    .addFormParam("param1", "value1")
                    .build();

            Response response = httpClient.executeRequest(request).get();
            System.out.println("HTTP response status code: " + response.getStatusCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
