package cn.test;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

public class LogServlet extends HttpServlet {

	private static Logger logger = Logger.getLogger(LogServlet.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String qs = URLDecoder.decode(request.getQueryString(), "utf-8");//获取所有request信息
		String []  attrs = qs.split("\\&");
		StringBuffer buf = new StringBuffer();
		for(String attr : attrs){
			String [] kv = attr.split("=");
			String key = kv[0];
			String value = kv.length == 2 ? kv[1] : "";
			buf.append(value+"|");
		}
		buf.append(request.getRemoteAddr());//获取ip地址
		
		String log = buf.toString();
		logger.info(log);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

}
