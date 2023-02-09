package ibm.eda.kc.orderms.domain;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import ibm.eda.kc.orderms.infra.events.order.OrderEventProducer;
import ibm.eda.kc.orderms.infra.repo.OrderRepository;
import io.opentelemetry.context.Context;


@ApplicationScoped
public class OrderService {

    @Inject
	public OrderRepository repository;

	@Inject
	public OrderEventProducer producer;

    public List<ShippingOrder> getAllOrders() {
		return repository.getAll();
	}


    @Transactional
    public ShippingOrder createOrder(ShippingOrder order) {
        if (order.orderID == null) {
            order.orderID = UUID.randomUUID().toString();
        }
        if (order.creationDate == null) {
			order.creationDate = LocalDate.now().toString();
		}
        order.status = ShippingOrder.PENDING_STATUS;
		order.updateDate= order.creationDate;
        repository.addOrder(order);
		producer.sendOrderCreatedEventFrom(order);

        return order;
    }


    public ShippingOrder getOrderById(String id) {
        return repository.findById(id);
    }


    @Transactional
    public void cancelOrder(ShippingOrder order) {
        order.status = ShippingOrder.CANCELLED_STATUS;
        producer.sendOrderUpdateEventFrom(order, Optional.empty());
        repository.updateOrder(order);
    }
}
