package org.eclipse.kura.stormseeker.tempsubscriber;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.eclipse.kura.cloudconnection.listener.CloudConnectionListener;
import org.eclipse.kura.cloudconnection.message.KuraMessage;
import org.eclipse.kura.cloudconnection.subscriber.CloudSubscriber;
import org.eclipse.kura.cloudconnection.subscriber.listener.CloudSubscriberListener;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemperatureSubscriber implements ConfigurableComponent, CloudConnectionListener, CloudSubscriberListener {

	//Variables
	private static final Logger s_logger = LoggerFactory.getLogger(TemperatureSubscriber.class);
	private Map<String, Object> properties;

	private CloudSubscriber cloudSubscriber;
	private final ScheduledExecutorService worker;
	private ScheduledFuture<?> handle;

	//Constructor
	public TemperatureSubscriber() {
		super();
		this.worker = Executors.newSingleThreadScheduledExecutor();
	}

	//Set and Unset methods
	public void setCloudSubscriber(CloudSubscriber cloudSubscriber) {
		this.cloudSubscriber = cloudSubscriber;
		this.cloudSubscriber.registerCloudSubscriberListener(TemperatureSubscriber.this);
		this.cloudSubscriber.registerCloudConnectionListener(TemperatureSubscriber.this);
	}

	public void unsetCloudSubscriber(CloudSubscriber cloudSubscriber) {
		this.cloudSubscriber.unregisterCloudSubscriberListener(TemperatureSubscriber.this);
		this.cloudSubscriber.unregisterCloudConnectionListener(TemperatureSubscriber.this);
		this.cloudSubscriber = null;
	}

	//Activation API
	protected void activate(ComponentContext componentContext, Map<String, Object> properties) {
		s_logger.info("Temp Sub has started with config!");
		//updated(properties);
		this.properties = properties;

		for (Entry<String, Object> property : properties.entrySet()) {
			s_logger.info("Update - {}: {}", property.getKey(), property.getValue());
		}

		s_logger.info("Activating Temp... Done.");
	}

	protected void deactivate(ComponentContext componentContext) {
		s_logger.info("Tem Sub has stopped!");
		this.worker.shutdown();
	}

	public void updated(Map<String, Object> properties) {
		this.properties = properties;
		for (Entry<String, Object> property : properties.entrySet()) {
			s_logger.info("Update - {}: {}", property.getKey(), property.getValue());
		}
		// try to kick off a new job
		//doUpdate(true);
		s_logger.info("Updated Temp... Done.");
	}

	/// Cloud Application Callback Methods

	@Override
	public void onConnectionEstablished() {
		s_logger.info("Connection established");
	}

	@Override
	public void onConnectionLost() {
		s_logger.warn("Connection lost!");
	}

	@Override
	public void onDisconnected() {
		s_logger.warn("On disconnected");
	}

	//Method called everytime a new message arrives
	@Override
	public void onMessageArrived(KuraMessage message) {
		logReceivedMessage(message);
		// TODO Auto-generated method stub

	} 

	//private methods
	private void logReceivedMessage(KuraMessage msg) {
		KuraPayload payload = msg.getPayload();
		Date timestamp = payload.getTimestamp();
		if (timestamp != null) {
			s_logger.info("Message timestamp: {}", timestamp.getTime());
		}

		if (payload.metrics() != null) {
			for (Entry<String, Object> entry : payload.metrics().entrySet()) {
				s_logger.info("Message metric: {}, value: {}", entry.getKey(), entry.getValue());
			}
		}
	}

}
