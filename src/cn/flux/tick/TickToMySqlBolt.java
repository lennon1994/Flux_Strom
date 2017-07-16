package cn.flux.tick;

import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.domain.TickTongjiInfo;
import cn.flux.dao.MySqlDao;

public class TickToMySqlBolt extends BaseRichBolt {
	
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		TickTongjiInfo info = new TickTongjiInfo();
		info.setTickTime(new Timestamp(input.getLongByField("time")));
		info.setBr(input.getDoubleByField("br"));
		info.setAvgtime(input.getDoubleByField("avgtime"));
		info.setAvgdeep(input.getDoubleByField("avgdeep"));
		MySqlDao.getMySqlDao().tickFlushData(info );
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	
	}

}
