package com.bigdata.tech.rpc.impl;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

import com.bigdata.tech.rpc.Bizable;

public class BizImpl implements Bizable {

	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public String hello(String name) {
		System.out.println("服务端被调用了");
		return "Hello "+name;
	}

}
