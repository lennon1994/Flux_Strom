package cn.flux.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import cn.domain.TickTongjiInfo;
import cn.domain.TongjiInfo;

public class MySqlDao {
	private static MySqlDao mySqlDao = new MySqlDao();
	private MySqlDao(){}

	public static MySqlDao getMySqlDao(){
		return mySqlDao;
	}
	
	public void tickFlushData(TickTongjiInfo info){
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			//1.注册数据库驱动
			Class.forName("com.mysql.jdbc.Driver");
			//2.获取数据库连接
			conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/fluxdb","root","root");
			//3.获取传输器对象
			ps = conn.prepareStatement("insert into tongji3 values (?,?,?,?)");
			ps.setTimestamp(1, info.getTickTime());
			ps.setDouble(2, info.getBr());
			ps.setDouble(3, info.getAvgtime());
			ps.setDouble(4, info.getAvgdeep());
			//4.执行
			ps.executeUpdate();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			//5.关闭资源
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}finally {
					ps = null;
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				} finally {
					conn = null;
				}
			}
		}
	}

	public void flushData(TongjiInfo info){
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			//1.注册数据库驱动
			Class.forName("com.mysql.jdbc.Driver");
			//2.获取数据库连接
			conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/fluxdb","root","root");
			//3.获取传输器对象
			ps = conn.prepareStatement("insert into tongji2 values (?,?,?,?,?,?)");
			ps.setTimestamp(1, info.getSstime());
			ps.setInt(2, info.getPv());
			ps.setInt(3, info.getUv());
			ps.setInt(4, info.getVv());
			ps.setInt(5, info.getNewip());
			ps.setInt(6, info.getNewcust());
			
			//4.执行
			ps.executeUpdate();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			//5.关闭资源
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}finally {
					ps = null;
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				} finally {
					conn = null;
				}
			}
		}
	}
}
