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

public class NewCustBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			//1.获取当前日志的uvid
			String uvid = input.getStringByField("uvid");
			//2.基于uvid查询hbase
			List<FluxInfo> list = HBaseDao.getHbaseDao().queryByField("uvid", uvid);
			//3.如果找得到 newip为0 否则为1
			int newcust = list.size() == 0 ? 1 : 0;
			//4.发送数据
			List<Object> values = input.getValues();
			values.add(newcust);
			collector.emit(values);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv","newip","newcust"));
	}

}
