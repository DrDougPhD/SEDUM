package routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import routing.chitchat.ChitChatRouter;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.MessageListener;
import core.Settings;
import core.SimClock;


public class SEDUMRouter extends ActiveRouter {
	
		private List<Integer> connectedNodesFromPreviousEpoch;
	private Map<Integer, DurationUtility> durationUtilities;
	private int epochStart;
	private Map<Integer, Integer> startingTimeOfConnection;
	private Map<Integer, Integer> connectionDurations;
	private Map<Integer, Integer> lastConnectionTime;
	private Map<Integer, Integer> lastUpdatedTime;


	private static final int DENIED = -22;
	
	/** SEDUM router's setting namespace ({@value})*/ 
	public static final String SEDUM_NS = "SEDUMRouter";

	public static final String EPOCH_DURATION_S = "epochDuration";
	public static final String WEIGHT_CONSTANT_S = "weightConstant";
	
	private static Double WEIGHT_CONSTANT;
	private static int EPOCH_DURATION;
	
	public SEDUMRouter(Settings s) {
		super(s);
	}
	
	public SEDUMRouter(ActiveRouter r) {
		super(r);
	}

	@Override
	public MessageRouter replicate() {
		return new SEDUMRouter(this);
	}

	@Override
	public void init(DTNHost host, List<MessageListener> mListeners) {
		super.init(host, mListeners);
		
		connectedNodesFromPreviousEpoch = new ArrayList<Integer>();
		durationUtilities = new HashMap<Integer, DurationUtility>();
		epochStart = SimClock.getIntTime();
		startingTimeOfConnection = new HashMap<Integer, Integer>();
		lastUpdatedTime = new HashMap<Integer, Integer>();
		lastConnectionTime = new HashMap<Integer, Integer>();
		connectionDurations = new HashMap<Integer, Integer>();
		
		Settings s = new Settings(SEDUM_NS);
		EPOCH_DURATION = s.getInt(EPOCH_DURATION_S, 1);
		WEIGHT_CONSTANT = s.getDouble(WEIGHT_CONSTANT_S, 0.2);
	}
	
	@Override
	public void update() {
		super.update();
		
		// If new time period has occurred
		if (newTimeEpoch()) {
			boolean printStuff = (getHost().getAddress() == 35 || getHost().getAddress() == 36);
			if (printStuff) {
				System.out.printf("%d at time %d: update\n", getHost().getAddress(), SimClock.getIntTime());
			}
			
			// Connections that are currently open have not seen an update of
			//  direct durations. Update thoses now.
			for (Connection c: getConnections()) {
				Integer j = c.getOtherNode(getHost()).getAddress();
				
				if (printStuff) {
					System.out.printf("\tConnection between %d and %d\n", getHost().getAddress(), j);
				}
				
				updateCumulativeDuration(j);
				startingTimeOfConnection.put(j, SimClock.getIntTime());
			}
			
			// for each meeting node j in the last time period T
			for (Integer j: connectedNodesFromPreviousEpoch) {
				
				if (printStuff) {
					System.out.printf("\tUpdating utility for %d\n", j);
				}
				
				// Calculate duration utility for current time period.
				DurationUtility currentDurationUtility = calculateCurrentUtility(j);
				
				// if u(i, j) exists in utility table
				if (durationUtilities.containsKey(j)) {
					// Update duration utility.
					DurationUtility newDurationUtility = calculateUpdatedUtility(j, currentDurationUtility);
					durationUtilities.put(j, newDurationUtility);					
				} else {
					durationUtilities.put(j, currentDurationUtility);
				}
				
				lastUpdatedTime.put(j, SimClock.getIntTime());
			}
			connectedNodesFromPreviousEpoch.clear();
			connectionDurations.clear();
			
			// Connections that still persist after an update will still
		    //  still be recognized as connected.
			for (Connection c: getConnections()) {				
				Integer j = c.getOtherNode(getHost()).getAddress();
				connectedNodesFromPreviousEpoch.add(j);
			}
		}
	}

	private DurationUtility calculateCurrentUtility(Integer j) {
		// Formula 10 in the paper.
		DurationUtility maxDurationUtility = new DirectDurationUtility(
			connectionDurations.get(j), EPOCH_DURATION
		);
		
		for (Integer k: durationUtilities.keySet()) {
			DurationUtility toK = durationUtilities.get(k);
			if (toK.isRelayedBy(j)) {
				if (maxDurationUtility.isSmallerThan(toK)) {
					maxDurationUtility = toK;
				}
			}
		}
		
		return maxDurationUtility;
	}
	
	private DurationUtility calculateUpdatedUtility(Integer j, DurationUtility currentDurationUtility) {
		// Formula 11 in the paper
		DurationUtility oldDurationUtility = durationUtilities.get(j);
		return currentDurationUtility.updateFromOld(oldDurationUtility, WEIGHT_CONSTANT);
	}


	private boolean newTimeEpoch() {
		if (SimClock.getIntTime() == epochStart + EPOCH_DURATION) {
			epochStart = SimClock.getIntTime();
			return true;
		}
		
		return false;
	}

	@Override
	public void changedConnection(Connection con) {
		//eta.iteration_marker(SimClock.getIntTime());
		if (getHost().getAddress() == 35 || getHost().getAddress() == 36) {
			if (con.getOtherNode(getHost()).getAddress() == 35 || con.getOtherNode(getHost()).getAddress() == 36) {
				System.out.printf("%d at time %d: changedConnection to %s\n", getHost().getAddress(), SimClock.getIntTime(), con.isUp()? "UP":"DOWN");
			}
		}
		
		super.changedConnection(con);
		DTNHost other = con.getOtherNode(getHost());
		Integer j = other.getAddress();
		if (con.isUp()) {
			
			// Exchange utility scores that have been updated
			//   since previous time of meeting
			Map<Integer, DurationUtility> neighborUtilities;
			if (lastConnectionTime.containsKey(j)) {
				neighborUtilities = ((SEDUMRouter) other.getRouter())
					.getUtilitiesSince(lastConnectionTime.get(j), getHost().getAddress());
			} else {
				neighborUtilities = ((SEDUMRouter) other.getRouter())
					.getUtilities();
			}
			
			// If utility score between this node and other node
			//   exists in this node's utility table
			if (durationUtilities.containsKey(j)) {
				
				// for each node k in updated utilities
				DurationUtility directUtility = durationUtilities.get(j);
				for (Integer k: neighborUtilities.keySet()) {
					if (getHost().getAddress() != k) {
						DurationUtility indirectUtility = neighborUtilities.get(k);
						DurationUtility relayUtility = new IndirectDurationUtility(directUtility, indirectUtility, j);
						
						if (durationUtilities.containsKey(k)) {
							
							// if u(i, j)*u(j, k) > u(i, k):
							if (durationUtilities.get(k).isSmallerThan(relayUtility)) {
								
							    // u(i,k) needs updating
								durationUtilities.put(k, relayUtility);
							}
							
						} else {
							durationUtilities.put(k, relayUtility);
						}
					}
				}
			}
			
			// Record contact time with j
			connectedNodesFromPreviousEpoch.add(j);
			startingTimeOfConnection.put(j, SimClock.getIntTime());
			
		} else {
			
			// A disconnection has occurred. The duration of disconnections
			//  should be updated for this node.
			updateCumulativeDuration(j);
		}
	}

	private void updateCumulativeDuration(Integer j) {
		int startingTime = startingTimeOfConnection.remove(j);
		int duration = SimClock.getIntTime() - startingTime;
		int cumulatedDuration;
		
		if (connectionDurations.containsKey(j)) {
			cumulatedDuration = connectionDurations.get(j);
		} else {
			cumulatedDuration = 0;
		}
		
		connectionDurations.put(j, duration + cumulatedDuration);
		lastConnectionTime.put(j, SimClock.getIntTime());
	}

	private Map<Integer, DurationUtility> getUtilities() {
		return durationUtilities;
	}

	private Map<Integer, DurationUtility> getUtilitiesSince(Integer startTime, int requester) {
		// Build a map of duration utilities that have been updated since
		//  the start time.
		
		boolean printStuff = (requester == 35 || requester == 36);
		if (printStuff) {
			System.out.printf("\tSTART TIME is %d\n", startTime);
		}
		
		Map<Integer, DurationUtility> updatedUtilities = new HashMap<Integer, DurationUtility>();
		for (Integer j: durationUtilities.keySet()) {
			if (j != requester) {
				
				if (printStuff) {
					System.out.printf("\tUtility for %d was last updated at %d\n", j, lastUpdatedTime.get(j));
				}
				
				
				
				if (lastUpdatedTime.get(j) > startTime) {
					
					if (printStuff) {
						System.out.printf("\t%d is seeking duration utility for %d\n", getHost().getAddress(), j);
					}
					
					updatedUtilities.put(j, durationUtilities.get(j));
				}
			}
		}
		return updatedUtilities;
	}

	@Override
	protected int startTransfer(Message m, Connection con) {
		if (otherNodeIsAGoodCandidate(m, con)) {
			// If other's buffer is not full,
			//   Send message
			// If all messages in buffer are core-replicas
			//   Reject message
			// If receiving message is a non-core replica
			//   If no message has a utility lower than recipient
			//     Reject message
			//   else
			//     Replace message(s) with lowest utility with M
			// Else
			//   Replace message(s) with lowest utility with M
			return super.startTransfer(m, con);
		} else {
			return DENIED;
		}
	}
	
	private boolean otherNodeIsAGoodCandidate(Message m, Connection con) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Message messageTransferred(String id, DTNHost from) {
		//eta.iteration_marker(SimClock.getIntTime());
		//eta.ping("messageTransferred");
		
		Message m = super.messageTransferred(id, from);
		return m;
	}
}
