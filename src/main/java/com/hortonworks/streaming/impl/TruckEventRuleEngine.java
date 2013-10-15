package com.hortonworks.streaming.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streaming.impl.utils.EventMailer;

public class TruckEventRuleEngine implements Serializable {
	private static final long serialVersionUID = -5526455911057368428L;
	private static final Logger logger = LoggerFactory
			.getLogger(TruckEventRuleEngine.class);
	public static final int MAX_UNSAFE_EVENTS = 5;
	public Map<String, LinkedList<String>> driverEvents = new HashMap<String, LinkedList<String>>();
	String email = "you@yourdomain.com";
	String subject = "Unsafe Driving Alert";

	public TruckEventRuleEngine() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config.properties"));
			email = prop.getProperty("notification.email");
			subject = prop.getProperty("notification.subject");
			logger.info("Initializing rule engine with email: " + email
					+ " subject: " + subject);
		} catch (FileNotFoundException e) {
			logger.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
		} catch (IOException e) {
			logger.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
		}
	}

	public void processEvent(String event) {
		String[] pieces = event.split("\\|");
		Timestamp timestamp = Timestamp.valueOf(pieces[0]);
		String truckId = pieces[1];
		String driverId = pieces[2];
		String eventType = pieces[3];
		if (!driverEvents.containsKey(driverId))
			driverEvents.put(driverId, new LinkedList<String>());
		if (!eventType.equals("Normal")) {
			if (driverEvents.get(driverId).size() < MAX_UNSAFE_EVENTS)
				driverEvents.get(driverId).push(timestamp + " " + eventType);
			else {
				try {
					// In this case they've had more than 5 unsafe events
					logger.info("UNSAFE DRIVING DETECTED FOR DRIVER ID: "
							+ driverId);
					StringBuffer events = new StringBuffer();
					for (String unsafeEvent : driverEvents.get(driverId)) {
						events.append(unsafeEvent + "\n");
					}
					EventMailer.sendEmail(email, email, subject,
							"We've identified 5 unsafe driving events for driver: "
									+ driverId + "\n\n" + events.toString());
				} catch (Exception e) {
					logger.error("Error occured while sending notificaiton email: "
							+ e.getMessage());
				} finally {
					driverEvents.get(driverId).clear();
				}
			}
		}
	}
}
