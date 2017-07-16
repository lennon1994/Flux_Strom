package cn.flux.dao;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import cn.domain.FluxInfo;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;

public class HBaseDao {
	/* �������ģʽ
	 * ����3����Ϊ��ʹHBaseDao��Ϊ������������������ֻ��һ�����󣬱���ÿ�ε����ٴ������������ӣ�ռ����Դ
	 * ��ͨ�����У���д���췽����Ĭ����public�ģ�������д��private�Ĺ��췽�����������ʱ����Ĭ�ϵ��ù��췽����
	 * private�Ĺ��췽���������ⲿ��û�취new�������
	 */
	private static HBaseDao hbaseDao = new HBaseDao();
	private HBaseDao() {
	}
	
	public static HBaseDao getHbaseDao(){
		return hbaseDao;
	}
	
	/**
	 * ����Ϣд��hbase
	 * @param fi ��װ����־��Ϣ��bean
	 */
	public void saveToHbase(FluxInfo fi){
		HTable tab = null;
		try {
			//1.����hbase���ö���
			Configuration conf = new Configuration();
			//--������Ĭ������ ����Ҫ����zookeeper�ĵ�ַ �ͻ���ͨ������zookeeper��ȡԪ������Ϣ
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.����HTable����
			tab = new HTable(conf, "flux".getBytes());
			
			//3.����д�������
			Put put = new Put(fi.getRK().getBytes());
			put.add("cf1".getBytes(), "url".getBytes(), fi.getUrl().getBytes());
			put.add("cf1".getBytes(), "urlname".getBytes(), fi.getUrlname().getBytes());
			put.add("cf1".getBytes(), "uvid".getBytes(), fi.getUvid().getBytes());
			put.add("cf1".getBytes(), "ssid".getBytes(), fi.getSsid().getBytes());
			put.add("cf1".getBytes(), "sscount".getBytes(), fi.getSscount().getBytes());
			put.add("cf1".getBytes(), "sstime".getBytes(), fi.getSstime().getBytes());
			put.add("cf1".getBytes(), "cip".getBytes(), fi.getCip().getBytes());
			tab.put(put);
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			//4.�ر�����
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	public List<FluxInfo> queryByField(String field,String value){
		HTable tab = null;
		try {
			//1.��ȡ���ö���
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.��ȡ�����
			tab = new HTable(conf, "flux".getBytes());
			
			//3.ɨ������
			Scan scan = new Scan();
			Filter filter = new SingleColumnValueFilter("cf1".getBytes(), field.getBytes(), CompareOp.EQUAL, value.getBytes());
			scan.setFilter(filter);
			
			//4.�������
			List<FluxInfo> list = new ArrayList<>();
			ResultScanner rs = tab.getScanner(scan);
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				FluxInfo fi = new FluxInfo();
				fi.setUrl(new String(r.getValue("cf1".getBytes(), "url".getBytes())));
				fi.setUrlname(new String(r.getValue("cf1".getBytes(), "urlname".getBytes())));
				fi.setUvid(new String(r.getValue("cf1".getBytes(), "uvid".getBytes())));
				fi.setSsid(new String(r.getValue("cf1".getBytes(), "ssid".getBytes())));
				fi.setSscount(new String(r.getValue("cf1".getBytes(), "sscount".getBytes())));
				fi.setSstime(new String(r.getValue("cf1".getBytes(), "sstime".getBytes())));
				fi.setCip(new String(r.getValue("cf1".getBytes(), "cip".getBytes())));
				list.add(fi);
			}
			return list;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	/**
	 * ��hbase�в�ѯ����
	 * @param start ��ʼ�м�
	 * @param stop �������м�
	 * @param rk_regex Ҫ��ѯ���м����������
	 * @return
	 */
	public List<FluxInfo> queryByRange(byte [] start,byte [] stop,String rk_regex){
		HTable tab = null;
		try {
			//1.��ȡ���ö���
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.��ȡ�����
			tab = new HTable(conf, "flux".getBytes());
			
			//3.ɨ������
			Scan scan = new Scan();
			scan.setStartRow(start);
			scan.setStopRow(stop);
			Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rk_regex));
			scan.setFilter(filter);
			
			//4.�������
			List<FluxInfo> list = new ArrayList<>();
			ResultScanner rs = tab.getScanner(scan);
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				FluxInfo fi = new FluxInfo();
				//cf1--family���壬��url--Qualifier����
				fi.setUrl(new String(r.getValue("cf1".getBytes(), "url".getBytes())));
				fi.setUrlname(new String(r.getValue("cf1".getBytes(), "urlname".getBytes())));
				fi.setUvid(new String(r.getValue("cf1".getBytes(), "uvid".getBytes())));
				fi.setSsid(new String(r.getValue("cf1".getBytes(), "ssid".getBytes())));
				fi.setSscount(new String(r.getValue("cf1".getBytes(), "sscount".getBytes())));
				fi.setSstime(new String(r.getValue("cf1".getBytes(), "sstime".getBytes())));
				fi.setCip(new String(r.getValue("cf1".getBytes(), "cip".getBytes())));
				list.add(fi);
			}
			return list;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if(tab != null){
				try {
					tab.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
		}
	}
}

