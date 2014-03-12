package hanb.elasticsearch.beginner;

import java.io.BufferedReader;
import java.io.FileReader;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexTest {
	private static final Logger log = LoggerFactory.getLogger(CreateIndexTest.class);
	
	@Test
	public void testCreateIndex() throws Exception {
		Settings settings;
		Client client;
		String setting = "";
		String mapping = "";
		
		settings = ImmutableSettings
				.settingsBuilder()
				.build();
		
		client = buildClient(settings);
	    
		// market.json에서 settings와 mappings 영역만 따로 분리한 것이며, 실제 파일 위치경로만 맞춰주면 된다.
		setting = readTextFile("schema/market_settings.json");	    	    
	    mapping = readTextFile("schema/market_mappings.json");
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("open_market")
			.setSettings(setting)
			.addMapping("market", mapping)
			.execute()
			.actionGet();
		
		client.close();
		
		log.debug("{}", createIndexResponse.isAcknowledged());
	}
	
	/**
	 * file reader
	 * 
	 * @param file		input file path
	 * @return
	 * @throws Exception
	 */
	protected String readTextFile(String file) throws Exception {
		String ret = "";
		BufferedReader br = new BufferedReader(new FileReader(file));
		
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            line = br.readLine();
	        }
	        
	        ret = sb.toString();
	    } finally {
	        br.close();
	    }
	    
	    return ret;
	}
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * elasticsearch client TransportAddress 등록.<br>
     * 	cluster.node.list<br>
     * 
     * @param settings		client settings 정보.
     * @throws Exception
     */
	protected Client buildClient(Settings settings) throws Exception {
		TransportClient client = new TransportClient(settings);
		String nodes = "localhost:9300";	// 검색엔진 node ip:port 정보 작성.
		String[] nodeList = nodes.split(",");
		int nodeSize = nodeList.length;

		for (int i = 0; i < nodeSize; i++) {
			client.addTransportAddress(toAddress(nodeList[i]));
		}

		return client;
	}
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * InetSocketTransportAddress 등록.<br>
     * 
     * @param address		node 들의 ip:port 정보.
     * @return InetSocketTransportAddress
     * @throws Exception
     */
	private InetSocketTransportAddress toAddress(String address) {
		if (address == null) return null;
		
		String[] splitted = address.split(":");
		int port = 9300;
		
		if (splitted.length > 1) {
			port = Integer.parseInt(splitted[1]);
		}
		
		return new InetSocketTransportAddress(splitted[0], port);
	}
}