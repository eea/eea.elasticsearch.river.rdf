package  org.elasticsearch.app;
import static org.elasticsearch.node.NodeBuilder.*;

import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.eea_rdf.RDFRiver;
import org.elasticsearch.river.eea_rdf.RDFRiverModule;
import org.elasticsearch.river.eea_rdf.settings.EEASettings;
import org.elasticsearch.river.eea_rdf.support.Harvester;

import java.net.UnknownHostException;

public class Indexer {
    private volatile Harvester harvester;
    private volatile Thread harvesterThread;

    public static void main(String[] args){

        //Harvester harvester = new Harvester();
        //harvester.run();
        //Node node = nodeBuilder().client(true).node();


        //Client client = node.client();

        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9200));

        RiverSettings settings = (RiverSettings) ImmutableSettings.settingsBuilder()
                .put(ClusterName.SETTING, "myClusterName")
                //.put(, "9200")
                .build();

        RDFRiver rdfRiver = new RDFRiver(new RiverName("river", "_river"), settings, "indexTemp", client );

        client.close();
        //harvester.run();


    }
}