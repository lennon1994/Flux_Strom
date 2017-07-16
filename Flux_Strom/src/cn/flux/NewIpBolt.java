package cn.flux;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.domain.FluxInfo;
import cn.flux.dao.HBaseDao;

public class NewIpBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.��ȡ��ǰ��־��cip
			String cip = input.getStringByField("cip");
			//2.����cip��ѯhbase
			List<FluxInfo> list = HBaseDao.getHbaseDao().queryByField("cip", cip);
			//3.����ҵõ� newipΪ0 ����Ϊ1
			int newip = list.size() == 0 ? 1 : 0;
			//4.��������
			List<Object> values = input.getValues();
			values.add(newip);
			collector.emit(values);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv","newip"));
	}

}
