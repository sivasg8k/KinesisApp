package com.bigdata.poc1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
  public static class SplitSentence extends BaseBasicBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
    	
		
		
	}
  }

  public static class WordCount extends BaseBasicBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, Integer> counts = new HashMap<String, Integer>();
    private transient BufferedWriter writer;
    
    public WordCount() {
    	  File file = new File("word_count.txt");
	      
    	  try {
	    	  
	    	  	writer = new BufferedWriter(new FileWriter(file));
	      	} catch (IOException e) {
				e.printStackTrace();
	      	}
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      
      try
      {
         	StringTokenizer st = new StringTokenizer(sentence," ");
			while(st.hasMoreTokens()) {
				String word = st.nextToken();
					
				Integer count = counts.get(word);
			      if (count == null)
			        count = 0;
			      count++;
			      counts.put(word, count);
			      collector.emit(new Values(word, count));
			      writer.write("word---->" + word + " count------>" + count);
				  writer.newLine();
			      
			}
		
      } catch(Exception e) {
    	  e.printStackTrace();
      } finally {
    	  try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    //builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
