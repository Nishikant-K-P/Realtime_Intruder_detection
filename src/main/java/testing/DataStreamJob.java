/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testing;

//import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import common.Events;
import common.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.schema.SchemaBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.asynchttpclient.AsyncHttpClient;
//import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {


//	private static final org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(DataStreamJob.class);
	private final SourceFunction<RentlyEvents> source;
	private final SinkFunction<RentlyEvents> sink;

	private static final String DEFAULT_REGION = "us-west-2";
	private static final String DEFAULT_OUTPUT_STREAM = "OutputStream";
	private static final String APPLICATION_CONFIG_GROUP = "FlinkApplicationProperties";

	public DataStreamJob(SourceFunction<RentlyEvents> source, SinkFunction<RentlyEvents> sink) {
		this.source = source;
		this.sink = sink;
	}

//	private static ParameterTool loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
//		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
//		Properties flinkProperties = applicationProperties.get(APPLICATION_CONFIG_GROUP);
//		if (flinkProperties == null) {
//			throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.");
//		}
//		Map<String, String> map = new HashMap<>(flinkProperties.size());
//		flinkProperties.forEach((k, v) -> map.put((String) k, (String) v));
//		return ParameterTool.fromMap(map);
//
//	}
//
//	private static KinesisStreamsSink<RentlyEvents> createSink(ParameterTool applicationProperties) {
//		Properties outputProperties = new Properties();
//		outputProperties.setProperty(AWSConfigConstants.AWS_REGION, DEFAULT_REGION);
//
//
//
//		return KinesisStreamsSink.<RentlyEvents>builder()
//				.setKinesisClientProperties(outputProperties)
//				.setSerializationSchema(new RentlyEventsSerializer())
//				.setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
//				.setStreamName(applicationProperties.get("OutputStreamName", DEFAULT_OUTPUT_STREAM))
//				.setFailOnError(false)
//				.build();
//	}

//	class AsyncApiEnd extends RichAsyncFunction<RentlyEvents, Tuple4<Long, Events, Long, String>> {
//
//		private String stringUrl = "http://localhost:5000/api/process_string";
//		private URL url;
//		private HttpURLConnection connection;
//
//
//		@Override
//		public void open(OpenContext openContext) throws Exception {
//			url = new URL(stringUrl);
//			connection = (HttpURLConnection) url.openConnection();
//			connection.setRequestMethod("POST");
//			connection.setRequestProperty("Content-Type", "application/json");
//			connection.setDoOutput(true);
//		}
//
//		@Override
//		public void close() throws Exception {
//			connection.disconnect();
//		}
//
//		public CompletableFuture<String> callAPI(RentlyEvents rentlyEvents) throws IOException {
//			CompletableFuture<String> future = new CompletableFuture<>();
//			new Thread(() -> {
//				String requestBody = String.format("{\"string\": \"%s\"}", String.valueOf(rentlyEvents.hub_id));
//				byte[] requestBodyBytes = requestBody.getBytes(StandardCharsets.UTF_8);
//
//				try (OutputStream outputStream = connection.getOutputStream()) {
//					outputStream.write(requestBodyBytes);
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//				String status;
//
//				int responseCode = 0;
//				try {
//					responseCode = connection.getResponseCode();
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//				if (responseCode == HttpURLConnection.HTTP_OK) {
//					try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
//						StringBuilder responseBuilder = new StringBuilder();
//						String line;
//						while (true) {
//							try {
//								if (!((line = reader.readLine()) != null)) break;
//							} catch (IOException e) {
//								throw new RuntimeException(e);
//							}
//							responseBuilder.append(line);
//						}
//						status = responseBuilder.toString();
//						future.complete(status);
//					}catch (IOException e) {
//						throw new RuntimeException(e);
//					}
//				}
//
//			}).start();
//			return future;
//		}
//
//
//		@Override
//		public void asyncInvoke(RentlyEvents rentlyEvents, ResultFuture<Tuple4<Long, Events, Long, String>> resultFuture) throws Exception {
//			Future<String> result = callAPI(rentlyEvents);
//
//			CompletableFuture.supplyAsync(new Supplier<String>() {
//
//				@Override
//				public String get() {
//                    try {
//                        return result.get();
//                    } catch (InterruptedException | ExecutionException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//			}).thenAccept( (String apiResult) -> {
//				resultFuture.complete(Collections.singleton(new Tuple4<>(rentlyEvents.hub_id, rentlyEvents.event_type,rentlyEvents.activity_time, apiResult)));
//			});
//		}
//	}



	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		final ParameterTool applicationProperties = loadApplicationProperties(env);
		SourceFunction<RentlyEvents> source = new EventGenerator();
		SinkFunction<RentlyEvents> sink = new PrintSinkFunction<>();
		SinkFunction<Tuple3<Long, RentlyEvents, String>> sink1 = new PrintSinkFunction<>();


		DataStream<RentlyEvents> ds = env.addSource(source);

		DataStream<RentlyEvents> ds2 = ds.filter(new Myfilter());

		AsyncFunction<RentlyEvents, Tuple3<Long, RentlyEvents, String>> customAsyncFuntion = new AsyncHttpEndPoint();

		DataStream<Tuple3<Long, RentlyEvents, String>> result = AsyncDataStream.orderedWait(
				ds2,
				customAsyncFuntion,
				3000,
				TimeUnit.MILLISECONDS,
				2

		).setParallelism(2).disableChaining();
		result  = result.filter(new MyfilterStatus());
		result.addSink(sink1);
//		KinesisStreamsSink<Tuple3<Long, RentlyEvents, String>> sink3 = createSink(applicationProperties);
//		result.sinkTo(sink3);
//		ds2.addSink(sink);
//		DataStream<Tuple4<Long, Events, Long, String>> flagged = AsyncDataStream.unorderedWait(ds2, new AsyncApiEnd(), 2,
//				TimeUnit.SECONDS, 10);

		//ds2.addSink(sink);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	public static class Myfilter implements FilterFunction<RentlyEvents> {

		@Override
		public boolean filter(RentlyEvents rentlyEvents) throws Exception {
			if (rentlyEvents.event_type == Events.TAMPERING || rentlyEvents.event_type == Events.ALARM_TRIGGERED) {
				return true;
			};
			if (rentlyEvents.event_type == Events.PHYSICAL_UNLOCK &&
					rentlyEvents.user_type == User.HAND &&
					(Instant.ofEpochSecond(rentlyEvents.activity_time).atZone(ZoneOffset.UTC).getHour() >= 21 ||
							Instant.ofEpochSecond(rentlyEvents.activity_time).atZone(ZoneOffset.UTC).getHour() < 6) ) {
				return true;

			}
			return false;
		}
	}

	public static class MyfilterStatus implements FilterFunction<Tuple3<Long, RentlyEvents, String>> {

		@Override
		public boolean filter(Tuple3<Long, RentlyEvents, String> rentlyEvents) throws Exception {
			if (rentlyEvents.f2.contains("true")){
				return true;
			}
			return false;

		}
	}
}
