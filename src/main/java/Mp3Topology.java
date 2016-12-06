import bolts.MP3UrlReceiver;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.WordMp3Reader;

/**
 * Created by PaulZhang on 2015/11/20.
 */
public class Mp3Topology {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("mp3-reader", new WordMp3Reader());
        builder.setBolt("mp3-receiver", new MP3UrlReceiver(), 100).shuffleGrouping("mp3-reader");

        // 配置
        Config config = new Config();
        config.put("mp3File", "src/main/resources/words_mp3_url.txt");
        config.setDebug(true);

        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("MP3-Topology", config, builder.createTopology());
        Thread.sleep(1000000);
        cluster.shutdown();

    }
}
