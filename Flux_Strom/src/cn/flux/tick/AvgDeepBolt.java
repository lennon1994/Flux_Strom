package cn.flux.tick;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.domain.FluxInfo;
import cn.flux.dao.HBaseDao;

public class AvgDeepBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//1.��ȡ��ǰ����ʱ�� ��Ϊ ͳ�ƽ�����λ��
		long stop = input.getLongByField("time");
		//2.���ݴ���ʱ�� ��ǰ����15���� ��Ϊ ͳ�ƿ�ʼ��λ��
		long start = stop - 1000 * 60 *15;
		//3.��hbase�в�ѯ���ʱ����ڵ���������
		List<FluxInfo> list = HBaseDao.getHbaseDao().queryByRange((start+"").getBytes(), (stop+"").getBytes(), "^.*$");
		//4.������Щ���ݼ���avgtime
		Map<String,Set<String>> map = new HashMap<>();
		for(FluxInfo fi : list){
			String ssid = fi.getSsid();
			if(map.containsKey(ssid)){
				Set<String> set = map.get(ssid);
				set.add(fi.getUrlname());
			}else{
				Set<String> set = new HashSet<>();
				set.add(fi.getUrlname());
				map.put(ssid, set);
			}
		}
		
		int ssCount = map.size();
		int deep = 0;
		for(Map.Entry<String, Set<String>> entry : map.entrySet()){
			deep += entry.getValue().size();
		}
		
		double avgDeep = 0;
		if(ssCount != 0){
			avgDeep = Math.round(deep * 10000.0 / ssCount)/10000.0;
		}
		
		//5.��������
		List<Object> values = input.getValues();
		values.add(avgDeep);
		collector.emit(input,values);
		collector.ack(input);
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br","avgtime","avgdeep"));
	}

}
