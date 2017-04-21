package com.orangemile.hadoop.kerberos;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * @see http://henning.kropponline.de/2016/02/14/a-secure-hdfs-client-example/
 * @see http://appcrawler.com/wordpress/2015/06/18/examples-of-connecting-to-kerberos-hive-in-jdbc/
 * @see http://henning.kropponline.de/2016/02/14/a-secure-hdfs-client-example/
 * @see https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html
 * 
 * 
 * 
 * TODO: Are the *.xml files needed - can it be replaced with "fs.defaultFS" hdfs://...
 */
public class KerberosUtil {

	private static final String LOGIN_MODULE = "KerberosUtil";

	/**
	 * 
	 * @param principal
	 *            orange@HADOOP.ORANGEMILE.COM
	 * @param password
	 *            password
	 * @param realm
	 *            HADOOP.ORANGEMILE.COM
	 * @param kdc
	 *            hadoop.orangemile.com (key-distribution-center)
	 * @param hdfsRoot
	 *            /etc/hadoop/conf/
	 * @return org.apache.hadoop.FileSystem
	 * @throws LoginException
	 * @throws IOException
	 */
	public static FileSystem loginToHdfs(String principal, char[] password, String realm, String kdc, String hdfsRoot)
			throws LoginException, IOException {
		System.setProperty("java.security.krb5.realm", realm);
		System.setProperty("java.security.krb5.kdc", kdc);
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		System.setProperty("hadoop.home.dir", hdfsRoot);

		String krb5ConfPath = getResource("krb5.conf");
		checkArgument(krb5ConfPath != null, "krb5.conf file not found in classpath");

		System.setProperty("java.security.krb5.conf", krb5ConfPath);

		Configuration conf = new Configuration();
		conf.set("principal", principal);
		conf.set("debug", "true");
		conf.set("hadoop.security.authentication", "kerberos");

		String[] files = new String[] { "hdfs-site.xml", "core-site.xml", "yarn-site.xml", "ssl-client.xml", "ssl-server.xml" };
		for (String file : files) {
			String name = getResource(file);
			if (name != null) {
				conf.addResource(name);
			}
		}

		InMemoryMultiuserJaasConfiguration authConfig = InMemoryMultiuserJaasConfiguration.getInstance();
		javax.security.auth.login.Configuration.setConfiguration(authConfig);

		Map<String, String> options = new HashMap<>();
		options.put("client", "true");
		options.put("debug", "true");
		options.put("isInitiator", "true");
		options.put("useTicketCache", "true");

		AppConfigurationEntry[] authEntries = new AppConfigurationEntry[] { new AppConfigurationEntry(
				"com.sun.security.auth.module.Krb5LoginModule", LoginModuleControlFlag.REQUIRED, options) };
		authConfig.append(LOGIN_MODULE, authEntries);

		UserGroupInformation.setConfiguration(conf);

		LoginContext lc = kinit(principal, password);

		UserGroupInformation.loginUserFromSubject(lc.getSubject());

		FileSystem hdfs = FileSystem.get(conf);

		return hdfs;
	}

	/**
	 * kinit via code
	 */
	public static LoginContext kinit(String username, char[] password) throws LoginException {
		CallbackHandler handler = (callbacks) -> {
			for (Callback c : callbacks) {
				if (c instanceof NameCallback) {
					((NameCallback) c).setName(username);
				}
				if (c instanceof PasswordCallback) {
					((PasswordCallback) c).setPassword(password);
				}
			}
		};

		LoginContext lc = new LoginContext(LOGIN_MODULE, handler);
		lc.login();
		return lc;
	}

	/**
	 * Searches for filename in class path
	 */
	public static String getResource(String fileName) {
		URL url = KerberosUtil.class.getResource(fileName);
		if (url == null)
			return null;
		return url.getFile();
	}

}
