package ibm.eda.kc.orderms.infra.events.voyage;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

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
 * Listen to voyages topic to process voyage allocation
 */
@ApplicationScoped
public class VoyageAgent {

    Logger logger = Logger.getLogger(VoyageAgent.class.getName());

    @Inject
    OrderRepository repo;

    @Inject
    public OrderEventProducer producer;


    @Incoming("voyages")
    public CompletionStage<Void> processVoyageEvent(Message<VoyageEvent> messageWithVoyageEvent) {
        logger.info("Received voyage event for : " + messageWithVoyageEvent.getPayload().voyageID);
        VoyageEvent voyageEvent = messageWithVoyageEvent.getPayload();
        Optional<TracingMetadata> optionalTracingMetadata = TracingMetadata.fromMessage(messageWithVoyageEvent);
        if (optionalTracingMetadata.isPresent()) {
            TracingMetadata tracingMetadata = optionalTracingMetadata.get();
            Context context = tracingMetadata.getCurrentContext();
            try (Scope scope = context.makeCurrent()) {
                createProcessedVoyageEventSpan(voyageEvent, context);
                switch (voyageEvent.getType()) {
                    case VoyageEvent.TYPE_VOYAGE_ASSIGNED:
                        VoyageEvent voyageAssignEvent = processVoyageAssignEvent(voyageEvent, Optional.of(context));
                        break;
                    default:
                        break;
                }
            }
        }
        return messageWithVoyageEvent.ack();
    }

    private void createProcessedVoyageEventSpan(final VoyageEvent voyageEvent, final Context context) {
        final String spanName = MessageFormat.format("processed event[{0}]", voyageEvent.getType());
        SpanBuilder spanBuilder = TRACER.spanBuilder(spanName).setParent(context);
        Span span = spanBuilder.startSpan();
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String voyageEventJson = ow.writeValueAsString(voyageEvent);
            span.setAttribute("processed.event", voyageEventJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            span.end();
        }
    }

    @Transactional
    public VoyageEvent processVoyageAssignEvent(VoyageEvent ve, Optional<Context> optionalContext) {
        logger.info("In processVoyageAssignEvent");
        VoyageAllocated ra = (VoyageAllocated) ve.payload;
        ShippingOrder order = repo.findById(ra.orderID);
        if (order != null) {
            order.voyageID = ve.voyageID;
            if (order.containerID != null) {
                order.status = ShippingOrder.ASSIGNED_STATUS;
                producer.sendOrderUpdateEventFrom(order, optionalContext);
            }
            repo.updateOrder(order);
        } else {
            logger.warning(ra.orderID + " not found in repository");
        }

        return ve;
    }


    @Scheduled(cron = "{voyage.cron.expr}")
    void cronJobForVoyageAnswerNotReceived() {
        // badly done - brute force as of now
        for (ShippingOrder o : repo.getAll()) {
            if (o.status.equals(ShippingOrder.PENDING_STATUS)) {
                if (o.containerID != null) {
                    o.status = ShippingOrder.ONHOLD_STATUS;
                    producer.sendOrderUpdateEventFrom(o, Optional.empty());
                }
            }
        }
    }
}
