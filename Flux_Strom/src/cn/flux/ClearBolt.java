package cn.flux;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ClearBolt extends BaseRichBolt{
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			String value = input.getStringByField("str");
			String [] attrs = value.split("\\|");
			String url = attrs[0];
			String urlname = attrs[1]; 
			String uvid = attrs[13];
			String ssid = attrs[14].split("_")[0];
			String sscount = attrs[14].split("_")[1]; 
			String sstime = attrs[14].split("_")[2];
			String cip = attrs[15];
			collector.emit(input,new Values(url,urlname,uvid,ssid,sscount,sstime,cip));
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip"));
	}

}
