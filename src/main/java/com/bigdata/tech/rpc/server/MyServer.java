package com.bigdata.tech.rpc.server;


import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.bigdata.tech.rpc.impl.BizImpl;

public class MyServer {

	public static int PORT = 3242;
	public static long VERSION = 23234l;
	
	public static void main(String[] args) {
		//final Server server = //RPC.getProtocolProxy(BizImpl.class, VERSION, new InetSocketAddress(PORT) , new Configuration());
	}
}
