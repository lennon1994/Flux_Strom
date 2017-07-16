package cn.flux;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.domain.FluxInfo;
import cn.flux.dao.HBaseDao;

public class ToHbaseBolt extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			FluxInfo fi = new FluxInfo();
			fi.setUrl(input.getStringByField("url"));
			fi.setUrlname(input.getStringByField("urlname"));
			fi.setUvid(input.getStringByField("uvid"));
			fi.setSsid(input.getStringByField("ssid"));
			fi.setSstime(input.getStringByField("sstime"));
			fi.setSscount(input.getStringByField("sscount"));
			fi.setCip(input.getStringByField("cip"));
			HBaseDao.getHbaseDao().saveToHbase(fi);
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
