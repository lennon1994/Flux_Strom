package cn.flux;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cn.flux.tick.AvgDeepBolt;
import cn.flux.tick.AvgTimeBolt;
import cn.flux.tick.BrBolt;
import cn.flux.tick.TickToMySqlBolt;
import cn.flux.tick.TimeBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FluxTopology {
	public static void main(String[] args) throws Exception {
	
	//FluxTopology����
		//1.�����������
		//--����KafkaSpout�������Լ���spout
		BrokerHosts hosts = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");
		SpoutConfig conf = new SpoutConfig(hosts, "flux", "/flux", UUID.randomUUID().toString());
		//�����һ��flux��ʾtopic�����֣��ڶ�������kafka����Ϣ������zookeeper�е����֣�����uuid�����ظ�
		conf.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spout = new KafkaSpout(conf);
		//--����Bolt
		ClearBolt clearBolt = new ClearBolt();
		PvBolt pvBolt = new PvBolt();
		UvBolt uvBolt = new UvBolt();
		VvBolt vvBolt = new VvBolt();
		NewIpBolt newIpBolt = new NewIpBolt();
		NewCustBolt newCustBolt = new NewCustBolt();
		ToMySqlBolt toMySqlBolt = new ToMySqlBolt();
		ToHbaseBolt toHbaseBolt = new ToHbaseBolt();
		PrintBolt printBolt = new PrintBolt();
		//2.����������
		TopologyBuilder builder  = new TopologyBuilder();
		//3.��֯����
		builder.setSpout("Flux_Spout", spout);
		builder.setBolt("Clear_Bolt", clearBolt).shuffleGrouping("Flux_Spout");
		builder.setBolt("Pv_Bolt", pvBolt).shuffleGrouping("Clear_Bolt");
		builder.setBolt("Uv_Bolt", uvBolt).shuffleGrouping("Pv_Bolt");
		builder.setBolt("Vv_Bolt", vvBolt).shuffleGrouping("Uv_Bolt");
		builder.setBolt("New_Ip_Bolt", newIpBolt).shuffleGrouping("Vv_Bolt");
		builder.setBolt("New_Cust_Bolt", newCustBolt).shuffleGrouping("New_Ip_Bolt");
		builder.setBolt("To_MySql_Bolt", toMySqlBolt).shuffleGrouping("New_Cust_Bolt");
		builder.setBolt("To_Hbase_Bolt", toHbaseBolt).shuffleGrouping("New_Cust_Bolt");
		builder.setBolt("Print_Bolt", printBolt).shuffleGrouping("New_Cust_Bolt");
		//4.��������
		StormTopology topology = builder.createTopology();
	
	//FluxTickTopology����
		//1.�����������
		TimeBolt timeBolt = new TimeBolt();
		BrBolt brBolt = new BrBolt();
		AvgTimeBolt avgTimeBolt = new AvgTimeBolt();
		AvgDeepBolt avgDeepBolt = new AvgDeepBolt();
		TickToMySqlBolt tickToMySqlBolt = new TickToMySqlBolt();
		PrintBolt tickPrintBolt = new PrintBolt();
		//2.����������
		TopologyBuilder tickBuilder = new TopologyBuilder();
		//3.��֯���˽ṹ------����û��spout����Ϊ���Ǹ�һ��ʱ��ȥhbase�в��
		tickBuilder.setBolt("Time_Bolt", timeBolt);
		tickBuilder.setBolt("Br_Bolt", brBolt).shuffleGrouping("Time_Bolt");
		tickBuilder.setBolt("Avg_Time_Bolt", avgTimeBolt).shuffleGrouping("Br_Bolt");
		tickBuilder.setBolt("Avg_Deep_Bolt", avgDeepBolt).shuffleGrouping("Avg_Time_Bolt");
		tickBuilder.setBolt("Tick_To_MySql_Bolt", tickToMySqlBolt).shuffleGrouping("Avg_Deep_Bolt");
		tickBuilder.setBolt("Tick_Print_Bolt", tickPrintBolt).shuffleGrouping("Avg_Deep_Bolt");
		//4.��������
		StormTopology tickTopology = tickBuilder.createTopology();
		
		//5.�ύ����Ⱥ������
		//Config c = new Config();
		//StormSubmitter.submitTopology("Flux_Topology", c, topology);
		
		//5.�ύ����Ⱥ������ - ���ز���
		LocalCluster cluster = new LocalCluster();
		Config c = new Config();
		cluster.submitTopology("Flux_Topology", c, topology);
		cluster.submitTopology("Tick_Topology", c, tickTopology);
		
		Thread.sleep(1000 * 1000);
		cluster.killTopology("Flux_Topology");
		cluster.killTopology("Tick_Topology");
		cluster.shutdown();
	}
}
