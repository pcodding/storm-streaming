package com.hortonworks;

import javax.jms.Session;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TruckEventProcessorTopology {
	public static final String JMS_QUEUE_SPOUT = "sensor_data_spout";
	public static final String TRUCK_EVENT_RULE_BOLT = "truck_event_rule_bolt";

	public static void main(String[] args) throws Exception {
		JmsProvider jmsQueueProvider = new SpringJmsProvider(
				"jms-activemq.xml", "jmsConnectionFactory", "notificationQueue");
		// JMS Producer
		JmsTupleProducer producer = new JsonTupleProducer();
		// JMS Queue Spout
		JmsSpout queueSpout = new JmsSpout();
		queueSpout.setJmsProvider(jmsQueueProvider);
		queueSpout.setJmsTupleProducer(producer);
		queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
		queueSpout.setDistributed(true); // allow multiple instances

		TopologyBuilder builder = new TopologyBuilder();

		// spout with 5 parallel instances
		builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 5);
		builder.setBolt(TRUCK_EVENT_RULE_BOLT, new TruckEventRuleBolt())
				.shuffleGrouping(JMS_QUEUE_SPOUT);

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("truck-event-processor", conf,
				builder.createTopology());
//		Utils.sleep(60000);
//		cluster.killTopology("truck-event-processor");
//		cluster.shutdown();
	}
}
