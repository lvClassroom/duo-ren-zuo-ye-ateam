package com.orangemile.hadoop.kerberos;

import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.collect.Maps;

public class InMemoryMultiuserJaasConfiguration extends Configuration {

	private final Map<String, AppConfigurationEntry[]> _conf;

	private static InMemoryMultiuserJaasConfiguration instance;

	private InMemoryMultiuserJaasConfiguration(Map<String, AppConfigurationEntry[]> conf) {
		this._conf = conf;
	}

	public static synchronized InMemoryMultiuserJaasConfiguration getInstance() {
		if (instance == null) {
			instance = new InMemoryMultiuserJaasConfiguration(Maps.newConcurrentMap());
		}
		return instance;
	}

	public void append(String name, AppConfigurationEntry[] values) {
		_conf.put(name, values);
	}

	public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
		return _conf.get(name);
	}

}
