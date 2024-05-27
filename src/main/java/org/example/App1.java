package org.example;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Do some thinking; publish a message; do some more thinking.
 */
public class App1 {
  private String topic = "projects/test1-305123/topics/topic1";

  private static OpenTelemetrySdk setupTraceExporter() throws Exception {
    // Using default project ID and Credentials
    TraceConfiguration configuration = TraceConfiguration.builder().setDeadline(Duration.ofMillis(30000)).build();

    SpanExporter traceExporter = TraceExporter.createWithConfiguration(configuration);
    // Register the TraceExporter with OpenTelemetry
    return OpenTelemetrySdk.builder()
      .setTracerProvider(
        SdkTracerProvider.builder()
          .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
          .build())
      .buildAndRegisterGlobal();
  } // setupTraceExporter

  public void run() {
    try {
      OpenTelemetrySdk openTelemetrySdk = setupTraceExporter();
      Span span =
        openTelemetrySdk.getTracer(App1.class.getName())
          .spanBuilder("App1: Description")
          .setAttribute("service.name", "App1")
          .startSpan();
      span.makeCurrent();
      System.out.println("App1: Starting");
      // Sleep for a second
      Thread.sleep(1000);
      // Publish a message
      System.out.println("App1: Publishing a message");
      Publisher publisher = Publisher.newBuilder(topic).build();
      ByteString data = ByteString.copyFromUtf8("my-message");

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
        .setData(data)
        .putAttributes("XXX_traceid", span.getSpanContext().getTraceId())
        .putAttributes("XXX_spanid", span.getSpanContext().getSpanId())
        .build();
      ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
      ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<>() {
        public void onSuccess(String messageId) {
          System.out.println("App1: published with message id: " + messageId);
        }

        public void onFailure(Throwable t) {
          System.out.println("App1: failed to publish: " + t);
        }
      }, MoreExecutors.directExecutor());
      // Sleep for 1/2 second
      Thread.sleep(500);
      System.out.println("App1: Ending");
      span.end();
      CompletableResultCode completableResultCode =
        openTelemetrySdk.getSdkTracerProvider().shutdown();
      // wait till export finishes
      completableResultCode.join(10000, TimeUnit.MILLISECONDS);
      // End
    } catch(Exception e) {
      e.printStackTrace();
    }
  } // run

  public static void main(String[] args) {
    App1 app1 = new App1();
    app1.run();
  } // main
} // App1