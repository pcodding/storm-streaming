package com.hortonworks.streaming.impl.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hortonworks.streaming.impl.TruckEventRuleEngine;

public class TruckEventRuleBolt implements IRichBolt {
	private static final Logger logger = LoggerFactory
			.getLogger(TruckEventRuleBolt.class);
	OutputCollector collector;
	TruckEventRuleEngine ruleEngine = new TruckEventRuleEngine();

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		logger.info("Processing message: " + input.getString(0));
		ruleEngine.processEvent(input.getString(0));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
