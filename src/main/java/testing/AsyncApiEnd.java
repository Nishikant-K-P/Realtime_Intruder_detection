package testing;

import common.Events;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

class AsyncApiEnd extends RichAsyncFunction<RentlyEvents, Tuple4<Long, Events, Long, String>> {

    private String stringUrl = "http://localhost:5000/api/process_string";
    private URL url;
    private HttpURLConnection connection;


    @Override
    public void open(OpenContext openContext) throws Exception {

    }

    @Override
    public void close() throws Exception {
        connection.disconnect();
    }

    public CompletableFuture<String> callAPI(RentlyEvents rentlyEvents) throws IOException {
        url = new URL(stringUrl);
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        CompletableFuture<String> future = new CompletableFuture<>();
        new Thread(() -> {
            String requestBody = String.format("{\"string\": \"%s\"}", String.valueOf(rentlyEvents.hub_id));
            byte[] requestBodyBytes = requestBody.getBytes(StandardCharsets.UTF_8);

            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(requestBodyBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String status;

            int responseCode = 0;
            try {
                responseCode = connection.getResponseCode();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    StringBuilder responseBuilder = new StringBuilder();
                    String line;
                    while (true) {
                        try {
                            if (!((line = reader.readLine()) != null)) break;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        responseBuilder.append(line);
                    }
                    status = responseBuilder.toString();
                    future.complete(status);
                }catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }).start();
        return future;
    }


    @Override
    public void asyncInvoke(RentlyEvents rentlyEvents, ResultFuture<Tuple4<Long, Events, Long, String>> resultFuture) throws Exception {
        Future<String> result = callAPI(rentlyEvents);

        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }).thenAccept( (String apiResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple4<>(rentlyEvents.hub_id, rentlyEvents.event_type,rentlyEvents.activity_time, apiResult)));
        });
    }
}
