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
		//1.��ȡ��ǰ����ʱ�� ��Ϊ ͳ�ƽ�����λ��
		long stop = input.getLongByField("time");
		//2.���ݴ���ʱ�� ��ǰ����15���� ��Ϊ ͳ�ƿ�ʼ��λ��
		long start = stop - 1000 * 60 *15;
		//3.��hbase�в�ѯ���ʱ����ڵ���������
		List<FluxInfo> list = HBaseDao.getHbaseDao().queryByRange((start+"").getBytes(), (stop+"").getBytes(), "^.*$");
		//4.������Щ���ݼ���br
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
		//5.��������
		List<Object> values = input.getValues();//���ϲ㴫��������ȫ����ȡ���ٽ�br��������
		values.add(br);
		collector.emit(input,values);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","br"));
	}

}
