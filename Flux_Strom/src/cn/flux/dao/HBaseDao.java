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
	/* 单例设计模式
	 * 以下3行是为了使HBaseDao成为单例对象，整个程序中只有一个对象，避免每次调用再创建对象建立连接，占用资源
	 * 普通的类中，不写构造方法，默认是public的，而这里写了private的构造方法，创建类的时候是默认调用构造方法的
	 * private的构造方法，这样外部就没办法new这个类了
	 */
	private static HBaseDao hbaseDao = new HBaseDao();
	private HBaseDao() {
	}
	
	public static HBaseDao getHbaseDao(){
		return hbaseDao;
	}
	
	/**
	 * 将信息写入hbase
	 * @param fi 封装了日志信息的bean
	 */
	public void saveToHbase(FluxInfo fi){
		HTable tab = null;
		try {
			//1.创建hbase配置对象
			Configuration conf = new Configuration();
			//--其他用默认配置 至少要配置zookeeper的地址 客户端通过链接zookeeper获取元数据信息
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.创建HTable对象
			tab = new HTable(conf, "flux".getBytes());
			
			//3.向表中存入数据
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
			//4.关闭连接
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
			//1.获取配置对象
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.获取表对象
			tab = new HTable(conf, "flux".getBytes());
			
			//3.扫描数据
			Scan scan = new Scan();
			Filter filter = new SingleColumnValueFilter("cf1".getBytes(), field.getBytes(), CompareOp.EQUAL, value.getBytes());
			scan.setFilter(filter);
			
			//4.遍历结果
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
	 * 从hbase中查询数据
	 * @param start 开始行键
	 * @param stop 结束的行键
	 * @param rk_regex 要查询的行键的正则规则
	 * @return
	 */
	public List<FluxInfo> queryByRange(byte [] start,byte [] stop,String rk_regex){
		HTable tab = null;
		try {
			//1.获取配置对象
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			//2.获取表对象
			tab = new HTable(conf, "flux".getBytes());
			
			//3.扫描数据
			Scan scan = new Scan();
			scan.setStartRow(start);
			scan.setStopRow(stop);
			Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rk_regex));
			scan.setFilter(filter);
			
			//4.遍历结果
			List<FluxInfo> list = new ArrayList<>();
			ResultScanner rs = tab.getScanner(scan);
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				FluxInfo fi = new FluxInfo();
				//cf1--family列族，，url--Qualifier列名
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

