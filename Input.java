import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Implementation of spout which reads interaction-metadata from file
 * 
 * @author Kasper Grud Skat Madsen
 */
class Input implements IRichSpout {
	private SpoutOutputCollector _collector;
	private ArrayList<String> _inputContent;
	private int _inputContentPos;
	private String _file;
	
	public Input(String file) {
		_file = file;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_inputContent = new ArrayList<String>();
		_collector = collector;
		_inputContentPos = 0;
		
		try {
			// Open file
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(_file)));

            // Read content into array
			String line = "";
			while((line = reader.readLine()) != null)
				_inputContent.add(line);
			
			// Close file
			reader.close();
		} catch(IOException ex) {
		  ex.printStackTrace();
		}
	}

	/**
	 * Called by Storm, until job is paused or terminated
	 */
	@Override
	public void nextTuple() {
		
		// In case all input is read, do short pause to prevent spinning
		if (_inputContentPos >= _inputContent.size()) {
			try { Thread.sleep(100); } catch (InterruptedException ex) {}
			return;
		}
		
		// Parse data
		String[] input = _inputContent.get(_inputContentPos++).split("#");
		String shortGeohash = input[1].substring(0, 4);
		
		// Emit
		_collector.emit(new Values(input[0],shortGeohash,input[1],input[2]));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userid", "shortGeohash", "geohash", "timestamp"));
	}
	
	@Override
	public void close() {
		// NOP
	}

	@Override
	public void activate() {
		// NOP
	}

	@Override
	public void deactivate() {
		// NOP
	}


	@Override
	public void ack(Object msgId) {
		// NOP
	}

	@Override
	public void fail(Object msgId) {
		// NOP
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}