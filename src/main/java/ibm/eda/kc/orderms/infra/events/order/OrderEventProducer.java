package ibm.eda.kc.orderms.infra.events.order;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.reactive.messaging.TracingMetadata;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import ibm.eda.kc.orderms.domain.ShippingOrder;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;

@ApplicationScoped
public class OrderEventProducer {
    Logger logger = Logger.getLogger(OrderEventProducer.class.getName());

    @Channel("orders")
    public Emitter<OrderEvent> eventProducer;

    public void sendOrderCreatedEventFrom(ShippingOrder order) {
        OrderEvent oe = createOrderEvent(order);
        oe.type = OrderEvent.ORDER_CREATED_TYPE;
        OrderCreatedEvent oce = new OrderCreatedEvent(order.getDestinationAddress().getCity(), order.getPickupAddress().getCity());
        oe.payload = oce;
        sendOrder(oe.orderID, oe, Optional.empty());
    }

    public void sendOrderUpdateEventFrom(ShippingOrder order, Optional<Context> optionalContext) {
        OrderEvent oe = createOrderEvent(order);
        oe.type = OrderEvent.ORDER_UPDATED_TYPE;
        oe.status = order.status;
        OrderUpdatedEvent oce = new OrderUpdatedEvent();
        oce.reeferIDs = order.containerID;
        oce.voyageID = order.voyageID;
        oe.payload = oce;
        sendOrder(oe.orderID, oe, optionalContext);
    }


    public void sendOrder(String key, OrderEvent orderEvent, Optional<Context> optionalContext) {
        logger.info("key " + key + " order event " + orderEvent.orderID + " ts: " + orderEvent.timestampMillis);
        if(optionalContext.isPresent()) {
            sendOrderWithContext(key, orderEvent, optionalContext.get());
        } else {
            sendOrderWithContext(key, orderEvent, Context.current());
        }
    }

    private void sendOrderWithContext(String key, OrderEvent orderEvent, Context context) {
        final Span span = startSpan(orderEvent, context);
        try {
            final String orderEventJson = serializeOrderEvent(orderEvent);
            addEventAttributeToSpan(span, orderEventJson);
            sendOrderEventWithSpanContext(key, orderEvent, span);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            endSpan(span);
        }
    }

    private Span startSpan(OrderEvent orderEvent, Context context) {
        final String spanName = formatSpanName(orderEvent);
        final SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        return spanBuilder.startSpan();
    }

    private String formatSpanName(OrderEvent orderEvent) {
        return MessageFormat.format("produced event[{0}]", orderEvent.getType());
    }

    private String serializeOrderEvent(OrderEvent orderEvent) throws JsonProcessingException {
        final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(orderEvent);
    }

    private void addEventAttributeToSpan(Span span, String orderEventJson) {
        span.setAttribute("produced.event", orderEventJson);
    }

    private void sendOrderEventWithSpanContext(String key, OrderEvent orderEvent, Span span) {
        final Context spanContext = Context.current().with(span);
        try (Scope scope = spanContext.makeCurrent()) {
            eventProducer.send(Message.of(orderEvent)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder().withKey(key).build())
                    .withAck(() -> CompletableFuture.completedFuture(null))
                    .withNack(throwable -> CompletableFuture.completedFuture(null))
                    .addMetadata(TracingMetadata.withCurrent(spanContext))
            );
        }
    }

    private void endSpan(Span span) {
        span.end();
    }

    private OrderEvent createOrderEvent(ShippingOrder order) {
        OrderEvent oe = new OrderEvent();
        oe.customerID = order.customerID;
        oe.orderID = order.orderID;
        oe.productID = order.productID;
        oe.quantity = order.quantity;
        oe.status = order.status;
        return oe;

    }
}
