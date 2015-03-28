import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;
import group.ModuleGrouping;
import spouts.WordReader;

/**
 * User: Paul Zhang
 * Date: 15/3/27
 * Time: 下午2:55
 */
public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {
        // 定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
//        builder.setBolt("word-normalizer", new WordNormalizer(), 10).customGrouping("word-reader", new ModuleGrouping());
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
//        builder.setBolt("word-counter", new WordCounter(), 2).customGrouping("word-normalizer", new ModuleGrouping());
        // 配置
        Config config = new Config();
        config.put("wordsFile", "src/main/resources/words.txt");
        config.setDebug(true);

        // 运行拓扑
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topology", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
