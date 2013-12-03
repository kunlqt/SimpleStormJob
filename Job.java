import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Kasper Grud Skat Madsen
 */
public class Job {
	static boolean runLocal = true;
	public static void main(String[] args) {
	
		/**
		 * Create TopologyBuilder
		 * 	used to build topologies (jobs)
		 */
		TopologyBuilder tb = new TopologyBuilder();
		
		
		/**
		 * Define spout (input)
		 * 	Arguments: Name, Instance, Num Instances
		 */
		tb.setSpout("input", new Input("input.txt"), 1);
		
		
		/**
		 * Define bolt (worker units)
		 * 	Arguments: Name, Instance, Num Instances
		 * 	Grouping: All geohashes with same value is guaranteed to be sent to same instance of this bolt
		 */
		tb.setBolt("processor", new Processor(), 2)
			.fieldsGrouping("input", new Fields("shortGeohash"));
		
		
		/**
		 * Launch job
		 */
		if (runLocal) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("UniqueUsersJob", new Config(), tb.createTopology());
			
			// Shutdown job, after 10 seconds
			try { Thread.sleep(10000); } catch (InterruptedException ex) {}
			cluster.shutdown();
		} else {
			try {
				StormSubmitter.submitTopology("UniqueUsersJob", new Config(), tb.createTopology());	
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
