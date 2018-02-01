package  org.elasticsearch.app;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.log4j.Logger;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

//import org.elasticsearch.common.inject.AbstractModule;


/*import org.elasticsearch.river.eea_rdf.RDFRiver;
import org.elasticsearch.river.eea_rdf.RDFRiverModule;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;*/
//import org.elasticsearch.river.eea_rdf.support.Harvester;

import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;

public class Indexer {
    final static ESLogger logger = Loggers.getLogger(Indexer.class);
    //private volatile Harvester harvester;
    //private volatile Thread harvesterThread;

    public static void main(String[] args) throws IOException {

        //Harvester harvester = new Harvester();
        //harvester.run();
        //Node node = nodeBuilder().client(true).node();

        //Client client = node.client();

        /*Settings settings = ImmutableSettings.settingsBuilder()
                //.put(ClusterName.SETTING, "myClusterName")
                //.put(, "9200")
                .build();*/

        //RiverSettings settings =
        Indexer indexer = new Indexer();

        //InetAddress addr = InetAddress.getByName("127.0.0.1");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("user_rw", "rw_pass"));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 19200, "http")
                ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }));

        //MainResponse response = client.info();

        Harvester harvester = new Harvester();
        harvester.client(client).riverName("_river");

        harvester.run();

        /*Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress(addr, 9200));*/


        //RDFRiver rdfRiver = new RDFRiver( new RiverName("river", "_river"), (RiverSettings) settings, "indexTemp", client );


        client.close();
        //harvester.run();


    }

   /* protected void configure() {
        bind(River.class).to(RDFRiver.class).asEagerSingleton();
    }*/
}