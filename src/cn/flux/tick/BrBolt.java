package cn.flux.tick;

import java.util.HashMap;
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
import ring.util.codec__init;

public class BrBolt extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//1.获取当前触发时间 作为 统计结束的位置
		long stop = input.getLongByField("time");
		//2.根据触发时间 向前推算15分钟 作为 统计开始的位置
		long start = stop - 1000 * 60 *15;
		//3.到hbase中查询这个时间段内的所有数据
		List<FluxInfo> list = HBaseDao.getHbaseDao().queryByRange((start+"").getBytes(), (stop+"").getBytes(), "^.*$");
		//4.根据这些数据计算br
		Map<String,Integer> map = new HashMap<>();
		for(FluxInfo fi : list){
			String ssid = fi.getSsid();
			map.put(ssid, map.containsKey(ssid) ? map.get(ssid) + 1 : 1);
		}
		
		int ssCount = map.size();
		int brCount = 0;
		for(Map.Entry<String, Integer>entry : map.entrySet()){
			if(entry.getValue() == 1)brCount++;
		}
		
		double br = 0;
		if(ssCount != 0){
			br = Math.round(brCount * 10000.0 / ssCount)/10000.0;
		}
		//5.发送数据
		List<Object> values = input.getValues();//将上层传来的数据全部获取，再将br跟到后面
		values.add(br);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br"));
	}

}
