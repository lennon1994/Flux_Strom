package cn.flux.tick;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TimeBolt extends BaseRichBolt {

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60 * 15);//15����
		return conf;
		//��������ڷ���conf����ʱĬ�ϴ�����spout����stormĬ��bolt����Ҫspout��
	}
	
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long time = System.currentTimeMillis();
		collector.emit(input,new Values(time));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time"));
	}

}
