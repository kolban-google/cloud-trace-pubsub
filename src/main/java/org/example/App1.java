package org.example;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
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
  private String topic;

  /**
   * Setup our Cloud Trace environment
   * @return The environment for Cloud Trace.
   * @throws Exception
   */
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

  /**
   * Run our application.
   */
  public void run() {
    try {
      topic = System.getenv("TOPIC");
      if (topic == null) {
        System.err.println("Expected to find a TOPIC environment variable indicating the PubSub topic to which we should publish");
        return;
      }
      OpenTelemetrySdk openTelemetrySdk = setupTraceExporter();
      Span span =
        openTelemetrySdk.getTracer(App1.class.getName())
          .spanBuilder("App1: The publisher")
          .setAttribute("service.name", "App1")
          .startSpan();

      try (Scope scope = span.makeCurrent()) {
        System.out.println("App1: Starting");
        // Sleep for a second - do some busy work
        Thread.sleep(1000);

        // Publish a message
        System.out.println("App1: Publishing a message");
        Publisher publisher = Publisher.newBuilder(topic).build();
        ByteString data = ByteString.copyFromUtf8("my-message");
        span.addEvent("Starting publish");
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setData(data)
          .putAttributes("XXX_traceid", span.getSpanContext().getTraceId())
          .putAttributes("XXX_spanid", span.getSpanContext().getSpanId())
          .build();

        // Publish the message
        String messageId = publisher.publish(pubsubMessage).get();
        span.addEvent("published", Attributes.builder().put("messageId", messageId).build());

        // Augment our span with the new messageId
        span.setAttribute("messaging.message.id", messageId);
        System.out.println("App1: published with message id: " + messageId);

        // Sleep for 1/2 second - do some busy work
        Thread.sleep(500);
        System.out.println("App1: Ending");
      } finally {
        span.end();
      }
      CompletableResultCode completableResultCode = openTelemetrySdk.getSdkTracerProvider().forceFlush();
      // wait till export finishes
      completableResultCode.join(10000, TimeUnit.MILLISECONDS);
      // End
    } catch(Exception e) {
      e.printStackTrace();
    }
  } // run

  /**
   * The entry point into our application.
   * @param args Arguments passed into our application.
   */
  public static void main(String[] args) {
    new App1().run();
  } // main
} // App1