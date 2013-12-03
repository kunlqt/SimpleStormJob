import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * An implementation of a bolt, which calculates number of unique users in different
 * geographical regions
 * 
 * @author Kasper Grud Skat Madsen
 */
public class Processor implements IRichBolt {
	private HashMap<String, HashSet<Integer>> _geoHashToUniqueUsers;
	private long _processedTuples;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_geoHashToUniqueUsers = new HashMap<String, HashSet<Integer>>();
		_processedTuples = 0;
	}

	/**
	 * Called on each received tuple
	 * 	Contains: userid, short geohash, full geohash, timestamp
	 */
	@Override
	public void execute(Tuple input) {
		
		// Parse input (in this simple example, we don't need all data)
		String userid = input.getString(0);
		String shortGeohash = input.getString(1);
		
		// Add user to set of unique users
		if (!_geoHashToUniqueUsers.containsKey(shortGeohash))
			_geoHashToUniqueUsers.put(shortGeohash, new HashSet<Integer>());
		_geoHashToUniqueUsers.get(shortGeohash).add(Integer.valueOf(userid));
		
		// Print output (periodically)
		if (++_processedTuples % 10000 == 0) {
			
			// Calculate total number of unique users
			long totalUniqueUsers = 0;
			for (HashSet<Integer> h : _geoHashToUniqueUsers.values())
				totalUniqueUsers += h.size();
			
			System.out.println("Total unique users on processor :: " + totalUniqueUsers);
			System.out.println("Total processed tuples on processor :: " + _processedTuples);
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// NOP - As nothing is emitted from this bolt
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
