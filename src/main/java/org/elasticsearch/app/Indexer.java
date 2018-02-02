package  org.elasticsearch.app;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.app.logging.ESLogger;
import org.elasticsearch.app.logging.Loggers;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.net.InetAddress;

public class Indexer {
    private static final ESLogger logger = Loggers.getLogger(Harvester.class);

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
        harvester.client(client).riverName("_river")
                //TODO: getFromSettingsFile
                .index("_river");

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