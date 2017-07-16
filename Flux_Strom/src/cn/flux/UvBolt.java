package cn.flux;

import java.util.Calendar;
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

public class UvBolt extends BaseRichBolt{

private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	@Override
	public void execute(Tuple input) {
		try {
			//--��ȡuvid
			String uvid = input.getStringByField("uvid");
			//--��ȡsstime
			long endTime = Long.parseLong(input.getStringByField("sstime"));
			//--���ڵ�ǰʱ����ǰѰ�ҽ���0���ʱ��ֵ
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(endTime);
			calendar.set(Calendar.HOUR, 0);
			calendar.set(Calendar.MINUTE,0);
			calendar.set(Calendar.SECOND,0);
			calendar.set(Calendar.MILLISECOND,0);
			long beginTime = calendar.getTimeInMillis();
			
			//--��hbase�в�ѯ ����0�㵽��ǰ��־ʱ������� ��uvid�͵�ǰuvid��ͬ������
			List<FluxInfo> list = HBaseDao.getHbaseDao().queryByRange((beginTime+"").getBytes(), (endTime+"").getBytes(), "^\\d{13}_"+uvid+"_\\d{10}_\\d{2}$");
			//--����Ҳ��� ��˵��uvid�����һ�γ��� uvΪ1 �����ǵ�һ�γ��� ��uvΪ0
			int uv = list.size() == 0 ? 1 : 0;
			
			//--������
			List<Object> values = input.getValues();
			values.add(uv);
			collector.emit(input,values);
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv"));
	}

}
