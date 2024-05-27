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

  private class MyMessageReceiver implements MessageReceiver {

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      System.out.println("App2: Received a message");
      System.out.println("App2: Message content: " + pubsubMessage);
      ackReplyConsumer.ack();
      try {
        String traceId = pubsubMessage.getAttributesOrThrow("XXX_traceid");
        String spanId = pubsubMessage.getAttributesOrThrow("XXX_spanid");
        SpanContext spanContext = SpanContext.createFromRemoteParent(
          traceId,
          spanId, TraceFlags.getSampled(), TraceState.getDefault());
        Context context = Context.root().with(Span.wrap(spanContext));
        // Process the message
        Span span =
          openTelemetrySdk.getTracer(App2.class.getName())
            .spanBuilder("App2: Description")
            .setParent(context)
            .setAttribute("service.name", "App2")
            .startSpan();
        span.makeCurrent();


        //System.out.println("App2: Message attributes: " + pubsubMessage.getAttributesMap());
        // Sleep for 1/2 second
        Thread.sleep(500);
        System.out.println("App2: Processed the message");
        span.end();
        //openTelemetrySdk.getSdkTracerProvider().forceFlush();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  } // MyMessageReceiver

  public void run() {
    try {
      Subscriber subscriber;
      openTelemetrySdk = setupTraceExporter();
      System.out.println("App2: Starting");
      subscriber = Subscriber.newBuilder(subscriptionName, new MyMessageReceiver()).build();
      subscriber.startAsync().awaitRunning();
      System.out.println("App2: Subscriber listening");
      subscriber.awaitTerminated();
      System.out.println("App2: Subscriber terminated");
    } catch(Exception e) {
      e.printStackTrace();
    }
  } // run

  public static void main(String[] args) {
    App2 app2 = new App2();
    app2.run();
  } // main
} // App2