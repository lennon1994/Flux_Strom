package cn.flux;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			Fields fields = input.getFields();
			StringBuffer buf = new StringBuffer();
			buf.append("--");
			Iterator<String> it = fields.iterator();
			while(it.hasNext()){
				String key = it.next();
				Object value = input.getValueByField(key);
				buf.append("-"+key+":"+value+"-");
			}
			buf.append("--");
			
			System.out.println(buf.toString());
			
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
