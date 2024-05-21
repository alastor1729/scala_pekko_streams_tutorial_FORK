package tools;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * We use Apache HTTP client for graceful retry behaviour
 * <br>
 * Doc:
 * https://platform.openai.com/docs/guides/chat/chat-vs-completions
 * https://platform.openai.com/docs/guides/text-generation
 */
public class OpenAICompletions {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenAICompletions.class);

    // Add your API key, see: https://platform.openai.com/api-keys
    public static final String API_KEY = "***";

    public static final int RESPONSE_TIMEOUT_SECONDS = 180;

    public static void main(String[] args) throws IOException {
        String toTranslate = String.format("Translate the following subtitle text from English to German: %s", "This is fun.");

        String modelCompletions = "gpt-3.5-turbo-instruct";
        ImmutablePair<String, Integer> resultRaw = new OpenAICompletions().runCompletions(modelCompletions, toTranslate);
        LOGGER.info("Translation: {}", resultRaw.getLeft());
        LOGGER.info("Total tokens: {}", resultRaw.getRight());

        String modelChatCompletions = "gpt-3.5-turbo"; //gpt-4
        ImmutablePair<String, Integer> resultRawChat = new OpenAICompletions().runChatCompletions(modelChatCompletions, toTranslate);
        LOGGER.info("Chat translation: {}", resultRawChat.getLeft());
        LOGGER.info("Chat total tokens: {}", resultRawChat.getRight());
    }

    public ImmutablePair<String, Integer> runCompletions(String model, String prompt) {
        JSONObject requestParams = new JSONObject();
        requestParams.put("model", model);
        requestParams.put("prompt", prompt);
        requestParams.put("max_tokens", 1000);

        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // In the context of translations: control the degree of deviation from the source text
        // High temperature value: the model generates a more creative or expressive translation
        // Low temperature value:  the model generates a more literal or faithful translation
        requestParams.put("temperature", 0.2);
        String endpointURL = "https://api.openai.com/v1/" + "completions";
        return extractPayloadCompletions(postRequest(requestParams, endpointURL));
    }

    public ImmutablePair<String, Integer> runChatCompletions(String model, String prompt) {

        JSONArray messages = new JSONArray();
        JSONObject jo = new JSONObject();
        jo.put("role", "user");
        jo.put("content", prompt);
        messages.put(jo);

        JSONObject requestParams = new JSONObject();
        requestParams.put("model", model);
        requestParams.put("messages", messages);
        requestParams.put("max_tokens", 1000);

        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // In the context of translations: control the degree of deviation from the source text
        // High temperature value: the model generates a more creative or expressive translation
        // Low temperature value:  the model generates a more literal or faithful translation
        requestParams.put("temperature", 0.2);
        String endpointURL = "https://api.openai.com/v1/" + "chat/completions";
        return extractPayloadChatCompletions(postRequest(requestParams, endpointURL));
    }

    public String postRequest(JSONObject requestParams, String endpointURL) {
        HttpPost request = new HttpPost(endpointURL);
        request.setHeader("Authorization", "Bearer " + API_KEY);
        StringEntity requestEntity = new StringEntity(
                requestParams.toString(),
                ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);

        RequestConfig timeoutsConfig = RequestConfig.custom()
                .setResponseTimeout(Timeout.of(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(timeoutsConfig)
                .setRetryStrategy(new HttpRequestRetryStrategyOpenAI())
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

    private static ImmutablePair<String, Integer> extractPayloadChatCompletions(String jsonResponseChatCompletions) {
        LOGGER.info("Raw response JSON: {}", jsonResponseChatCompletions);
        JSONObject obj = new JSONObject(jsonResponseChatCompletions);

        // Check raw response to see cause of parsing ex
        JSONArray arr = obj.getJSONArray("choices");
        JSONObject msg = arr.getJSONObject(0);
        checkLength(msg);

        String content = msg.getJSONObject("message").getString("content");
        int totalTokens = obj.getJSONObject("usage").getInt("total_tokens");
        return new ImmutablePair<>(content, totalTokens);
    }

    private ImmutablePair<String, Integer> extractPayloadCompletions(String jsonResponseCompletions) {
        LOGGER.info("Raw response JSON: {}", jsonResponseCompletions);
        JSONObject obj = new JSONObject(jsonResponseCompletions);

        // Check raw response to see cause of parsing ex
        JSONArray arr = obj.getJSONArray("choices");
        JSONObject msg = arr.getJSONObject(0);
        checkLength(msg);

        String content = msg.getString("text");
        int totalTokens = obj.getJSONObject("usage").getInt("total_tokens");
        return new ImmutablePair<>(content, totalTokens);
    }

    private static void checkLength(JSONObject obj) {
        String finish_reason = obj.getString("finish_reason");
        if (finish_reason.equals("length")) {
            LOGGER.warn("finish_reason has value 'length'. Increase max_tokens to get full response.");
        }
    }
}