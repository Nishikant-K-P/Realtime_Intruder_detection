package testing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;


public class AsyncHttpEndPoint extends RichAsyncFunction<RentlyEvents, Tuple3<Long, RentlyEvents, String>> {
    private static final Logger log = LoggerFactory.getLogger(AsyncHttpEndPoint.class);
    String url = "http://localhost:5000/api/process_string";
    private AsyncHttpClient client;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        DefaultAsyncHttpClientConfig.Builder configBuilder = Dsl.config().setConnectTimeout(500);
        client = Dsl.asyncHttpClient(configBuilder.build());
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }

    @Override
    public void asyncInvoke(RentlyEvents rentlyEvents, ResultFuture<Tuple3<Long, RentlyEvents, String>> resultFuture) throws Exception {
//        String requestBody = String.format("{\"string\": \"%s\"}", String.valueOf(rentlyEvents.hub_id));
        String requestBody = "{\"hub_id\": \"" + rentlyEvents.hub_id + "\"}";
//        JSONPObject requestBody = new JSONP
        log.debug(requestBody);

        Future<Response> responseFuture = client.preparePost(url).setHeader("Content-Type", "application/json").setBody(requestBody).execute();

        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    Response response = responseFuture.get();
                    return response.getResponseBody();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }

            }
        }).thenAccept((String body) ->{
            if (body != null) {
                resultFuture.complete(Collections.singleton(Tuple3.of(rentlyEvents.hub_id, rentlyEvents, body)));
            }
        });

    }
}
