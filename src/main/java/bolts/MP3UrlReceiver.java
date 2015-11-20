package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mpatric.mp3agic.InvalidDataException;
import com.mpatric.mp3agic.Mp3File;
import com.mpatric.mp3agic.UnsupportedTagException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by PaulZhang on 2015/11/20.
 */
public class MP3UrlReceiver implements IRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
//        System.out.println("receiver:" + sentence);
        String fileName = sentence.substring(sentence.lastIndexOf("/") + 1, sentence.length());
//        System.out.println(fileName);
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet getMethod = new HttpGet(sentence);
        try {
            CloseableHttpResponse response = client.execute(getMethod);
            InputStream inputStream = response.getEntity().getContent();
            DataOutputStream output = new DataOutputStream(new FileOutputStream("D:/audio/" + fileName));

            byte[] buffer = new byte[1024 * 8];
            int count = 0;
            while ((count = inputStream.read(buffer)) > 0) {
                output.write(buffer, 0, count);
            }

            Mp3File file = new Mp3File("D:/audio/" + fileName);
            System.out.println("Length of this mp3 is: " + file.getLengthInMilliseconds() + " ms");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (UnsupportedTagException e) {
            e.printStackTrace();
        } catch (InvalidDataException e) {
            e.printStackTrace();
        }

        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
