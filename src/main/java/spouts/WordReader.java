package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 * User: Paul Zhang
 * Date: 15/3/27
 * Time: 下午2:00
 */
public class WordReader implements IRichSpout {

    private TopologyContext context;
    private FileReader fileReader;
    private SpoutOutputCollector collector;
    private boolean completed;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.context = topologyContext;
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file", e);
        }
        this.collector = spoutOutputCollector;

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // do nothing
            }

            return;
        }

        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null) {
                this.collector.emit(new Values(str));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }


    }

    @Override
    public void ack(Object o) {
        System.out.println("OK:" + o);

    }

    @Override
    public void fail(Object o) {
        System.out.println("FAIL:" + o);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
