package ibm.eda.kc.orderms.infra.events.reefer;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.smallrye.reactive.messaging.TracingMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import ibm.eda.kc.orderms.domain.ShippingOrder;
import ibm.eda.kc.orderms.infra.events.order.OrderEventProducer;
import ibm.eda.kc.orderms.infra.repo.OrderRepository;
import io.quarkus.scheduler.Scheduled;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;

/**
 * Listen to the reefer topic and processes event from reefer service:
 * - reefer allocated event
 * - reefer unavailable event
 */
@ApplicationScoped
public class ReeferAgent {
    Logger logger = Logger.getLogger(ReeferAgent.class.getName());

    @Inject
    OrderRepository repo;

    @Inject
    public OrderEventProducer producer;

    @Incoming("reefers")
    public CompletionStage<Void> processReeferEvent(Message<ReeferEvent> messageWithReeferEvent) {
        logger.info("In processReeferEvent");
        logger.info("Received reefer event for : " + messageWithReeferEvent.getPayload().reeferID);
        ReeferEvent reeferEvent = messageWithReeferEvent.getPayload();
        Optional<TracingMetadata> optionalTracingMetadata = TracingMetadata.fromMessage(messageWithReeferEvent);
        if (optionalTracingMetadata.isPresent()) {
            TracingMetadata tracingMetadata = optionalTracingMetadata.get();
            Context context = tracingMetadata.getCurrentContext();
            try (Scope scope = context.makeCurrent()) {
                createProcessedReeferEventSpan(reeferEvent, context);
                switch (reeferEvent.getType()) {
                    case ReeferEvent.REEFER_ALLOCATED_TYPE:
                        ReeferEvent reeferAllocatedEvent = processReeferAllocatedEvent(reeferEvent, Optional.of(context));
                        break;
                    default:
                        break;
                }
            }
        }
        return messageWithReeferEvent.ack();
    }

    private void createProcessedReeferEventSpan(final ReeferEvent reeferEvent, final Context context) {
        final String spanName = MessageFormat.format("processed event[{0}]", reeferEvent.getType());
        SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        Span span = spanBuilder.startSpan();
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String reeferEventJson = ow.writeValueAsString(reeferEvent);
            span.setAttribute("processed.event", reeferEventJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            span.end();
        }
    }


    /**
     * When order created, search for reefers close to the pickup location,
     * add them in the container ids and send an event as ReeferAllocated
     */
    public ReeferEvent processReeferAllocatedEvent(ReeferEvent re, Optional<Context> optionalContext) {
        logger.info("In processReeferAllocatedEvent");
        ReeferAllocated ra = (ReeferAllocated) re.payload;
        ShippingOrder order = repo.findById(ra.orderID);
        if (order != null) {
            order.containerID = ra.reeferIDs;
            if (order.voyageID != null) {
                order.status = ShippingOrder.ASSIGNED_STATUS;
                producer.sendOrderUpdateEventFrom(order, optionalContext);
            }
            repo.updateOrder(order);
        } else {
            logger.warning(ra.orderID + " not found in repository");
        }

        return re;
    }

    @Scheduled(cron = "{reefer.cron.expr}")
    void cronJobForReeferAnswerNotReceived() {
        // badly done - brute force as of now
        for (ShippingOrder o : repo.getAll()) {
            if (o.status.equals(ShippingOrder.PENDING_STATUS)) {
                if (o.voyageID != null) {
                    o.status = ShippingOrder.ONHOLD_STATUS;
                    producer.sendOrderUpdateEventFrom(o, Optional.empty());
                }
            }
        }
    }

}
