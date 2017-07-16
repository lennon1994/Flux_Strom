package cn.flux.tick;

import java.util.ArrayList;
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

public class AvgTimeBolt extends BaseRichBolt {

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
		//4.根据这些数据计算avgtime
		Map<String,List<FluxInfo>> map = new HashMap<>();
		for(FluxInfo fi : list){
			String ssid = fi.getSsid();
			if(map.containsKey(ssid)){
				map.get(ssid).add(fi);
			}else{
				List<FluxInfo> fiList = new ArrayList<>();
				fiList.add(fi);
				map.put(ssid, fiList);
			}
		}
		
		int ssCount = map.size();
		long useTime = 0;
		for(Map.Entry<String, List<FluxInfo>> entry : map.entrySet()){
			String ssid = entry.getKey();
			List<FluxInfo> fiList = entry.getValue();
			long maxTime = Long.MIN_VALUE;
			long minTime = Long.MAX_VALUE;
			for(FluxInfo fi : fiList){
				long ssTime = Long.parseLong(fi.getSstime());
				if(ssTime >= maxTime){
					maxTime = ssTime;
				}
				if(ssTime <= minTime){
					minTime = ssTime;
				}
			}
			useTime += maxTime - minTime;
		}
		
		double avgTime = 0;
		if(ssCount != 0){
			avgTime = Math.round(useTime * 10000.0 / ssCount)/10000.0;
		}
		
		//5.发送数据
		List<Object> values = input.getValues();
		values.add(avgTime);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br","avgtime"));
	}

}
