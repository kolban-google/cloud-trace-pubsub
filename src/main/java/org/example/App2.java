package org.example;

import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import com.google.cloud.pubsub.v1.Subscriber;

import java.time.Duration;

/**
 * Subscribe to a topic and get messages when they arrive.
 */
public class App2 {
  private String subscriptionName = "projects/test1-305123/subscriptions/subscription1";
  private OpenTelemetrySdk openTelemetrySdk;

  /**
   * Initialize the Cloud Trace service.
   *
   * @return
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
   * The message receiver is instantiated when a new message is available for our PubSub subscription.
   * We start a new Cloud Trace span and perform some work on the message.
   */
  private class MyMessageReceiver implements MessageReceiver {
    /**
     * Handle the message.
     *
     * @param pubsubMessage    The PubSub message received for the subscription.
     * @param ackReplyConsumer The message ACK processor.
     */
    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      try {
        // Get the traceId and spanId from the attributes of the message.
        String traceId = pubsubMessage.getAttributesOrThrow("XXX_traceid");
        String spanId = pubsubMessage.getAttributesOrThrow("XXX_spanid");

        // Create a new trace span using the traceId/spanId as received as the parent for the new span.
        SpanContext spanContext = SpanContext.createFromRemoteParent(
          traceId,
          spanId, TraceFlags.getSampled(), TraceState.getDefault());
        Context context = Context.root().with(Span.wrap(spanContext));
        Span span =
          openTelemetrySdk.getTracer(App2.class.getName())
            .spanBuilder("App2: The subscriber")
            .setParent(context)
            .setAttribute("service.name", "App2")
            .setAttribute("messaging.message.id", pubsubMessage.getMessageId())
            .startSpan();
        try (Scope scope = span.makeCurrent()) {

          System.out.println("App2: Received a message");
          System.out.println("App2: Message content: " + pubsubMessage);
          ackReplyConsumer.ack();

          // Sleep for 1/2 second; this represents us doing some useful work.
          Thread.sleep(500);

          System.out.println("App2: Processed the message");
        } finally {
          span.end();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } // receiveMessage
  } // MyMessageReceiver

  /**
   * Run the logic of our application.
   */
  public void run() {
    try {
      openTelemetrySdk = setupTraceExporter();

      System.out.println("App2: Starting");
      Subscriber subscriber = Subscriber.newBuilder(subscriptionName, new MyMessageReceiver()).build();
      subscriber.startAsync().awaitRunning();
      System.out.println("App2: Subscriber running");
      subscriber.awaitTerminated();
      System.out.println("App2: Subscriber terminated");
    } catch (Exception e) {
      e.printStackTrace();
    }
  } // run

  /**
   * Entry point into our application.
   *
   * @param args Arguments passed into our application.
   */
  public static void main(String[] args) {
    new App2().run();
  } // main
} // App2