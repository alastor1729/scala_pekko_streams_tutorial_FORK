package alpakka.sse_to_elasticsearch;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * PoC with Apache HTTP client
 * <br>
 * Doc:
 * https://beta.openai.com/docs/api-reference/completions/create
 */
public class NerRequestOpenAI {
    private static final Logger LOGGER = LoggerFactory.getLogger(NerRequestOpenAI.class);

    // API key: https://beta.openai.com/account/api-keys
    public static final String API_KEY = "***";

    public static final int DELAY_TO_RETRY_SECONDS = 10;

    public static void main(String[] args) {
        String nerResult = new NerRequestOpenAI().run("Paul A. Bernet does not work there, but Mark Zuckerberg is the CEO of Facebook, which is based in Menlo Park, California.");
        LOGGER.info("NER result: {}", nerResult);
    }

    public String run(String text) {
        JSONObject requestParams = getRequestParam(text);

        String endpointURL = "https://api.openai.com/v1/completions";
        HttpPost request = new HttpPost(endpointURL);
        request.setHeader("Authorization", "Bearer " + API_KEY);
        StringEntity requestEntity = new StringEntity(
                requestParams.toString(),
                ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);

        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create();
        connectionManagerBuilder.setDefaultConnectionConfig(ConnectionConfig.custom()
                .setSocketTimeout(Timeout.of(DELAY_TO_RETRY_SECONDS, TimeUnit.SECONDS))
                .build());

        try (CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setConnectionManager(connectionManagerBuilder.build())
                .setRetryStrategy(new DefaultHttpRequestRetryStrategy(3, TimeValue.ofMinutes(1L)))
                .build()) {
            return httpClient.execute(request, response -> {
                HttpEntity entity = response.getEntity();
                return entity != null ? EntityUtils.toString(entity) : "N/A";
            });
        } catch (IOException e) {
            LOGGER.warn("Connection issue while accessing openai API endpoint: {}. Cause: ", endpointURL, e);
            throw new RuntimeException(e);
        }
    }

    private @NotNull JSONObject getRequestParam(String text) {
        JSONObject requestParams = new JSONObject();
        requestParams.put("model", "text-davinci-003");
        requestParams.put("prompt", "Named Entity Recognition on: " + text);
        // For testing use lower number to keep the usage low
        requestParams.put("max_tokens", 40);
        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // For NER this means we get not just 'Person' but also 'Organisation', 'Location'
        requestParams.put("temperature", 0.2);
        return requestParams;
    }
}
