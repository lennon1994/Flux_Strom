package cn.flux;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.domain.TongjiInfo;
import cn.flux.dao.MySqlDao;

public class ToMySqlBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			TongjiInfo info = new TongjiInfo();
			Timestamp stamp = new Timestamp(Long.parseLong(input.getStringByField("sstime")));
			info.setSstime(stamp);
			info.setPv(input.getIntegerByField("pv"));
			info.setUv(input.getIntegerByField("uv"));
			info.setVv(input.getIntegerByField("vv"));
			info.setNewip(input.getIntegerByField("newip"));
			info.setNewcust(input.getIntegerByField("newcust"));
			MySqlDao.getMySqlDao().flushData(info);
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
