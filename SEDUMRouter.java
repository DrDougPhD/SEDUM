package routing;

import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	/** The messages this router knows has been delivered */
	private Map<String, Message> knownDeliveredMessages;
	private Set<String> coreReplicas;

	private static final int DENIED = -22;

	/** SEDUM router's setting namespace ({@value})*/ 
	public static final String SEDUM_NS = "SEDUMRouter";

	public static final String EPOCH_DURATION_S = "epochDuration";
	public static final String WEIGHT_CONSTANT_S = "weightConstant";
	public static final String NUM_REPLICAS_S = "numberOfReplicas";

	private static Double WEIGHT_CONSTANT;
	private static int EPOCH_DURATION;
	private static Integer NUM_REPLICAS;

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
		knownDeliveredMessages = new HashMap<String, Message>();
		coreReplicas = new HashSet<String>();

		Settings s = new Settings(SEDUM_NS);
		EPOCH_DURATION = s.getInt(EPOCH_DURATION_S, 1);
		WEIGHT_CONSTANT = s.getDouble(WEIGHT_CONSTANT_S, 0.2);
		NUM_REPLICAS = s.getInt(NUM_REPLICAS_S, 0);
	}

	@Override
	public void update() {
		System.out.printf("%d at time %d: update\n", getHost().getAddress(), SimClock.getIntTime());
		//super.update();

		// If new time period has occurred
		if (newTimeEpoch()) {
			/*boolean printStuff = (getHost().getAddress() == 36 || getHost().getAddress() == 13);
			if (printStuff) {
				System.out.printf("%d at time %d: update\n", getHost().getAddress(), SimClock.getIntTime());
			}*/

			// Connections that are currently open have not seen an update of
			//  direct durations. Update thoses now.
			for (Connection c: getConnections()) {
				Integer j = c.getOtherNode(getHost()).getAddress();
				/*
				if (printStuff) {
					System.out.printf("\tConnection between %d and %d\n", getHost().getAddress(), j);
				}
				 */
				updateCumulativeDuration(j);
				startingTimeOfConnection.put(j, SimClock.getIntTime());
			}

			// for each meeting node j in the last time period T
			for (Integer j: connectedNodesFromPreviousEpoch) {
				/*
				if (printStuff) {
					System.out.printf("\tUpdating utility for %d\n", j);
				}
				 */
				// Calculate duration utility for current time period.
				DurationUtility currentDurationUtility = calculateCurrentUtility(j);

				// if u(i, j) exists in utility table
				if (durationUtilities.containsKey(j)) {
					// Update duration utility.
					DurationUtility newDurationUtility =
							calculateUpdatedUtility(j, currentDurationUtility);
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

		// Now try to exchange messages!
		//super.update();
		if (isTransferring() || !canStartTransfer()) {
			return; // transferring, don't try other connections yet
		}

		// Try first the messages that can be delivered to final recipient
		if (exchangeDeliverableMessages() != null) {
			return; // started a transfer, don't try others (yet)
		}

		// then try any/all message to any/all connection
		this.tryAllMessagesToAllConnections();
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
		System.out.printf("%d at time %d: changedConnection to %s\n", getHost().getAddress(), SimClock.getIntTime(), con.isUp()? "UP":"DOWN");
		//eta.iteration_marker(SimClock.getIntTime());
		/*if (getHost().getAddress() == 36 || getHost().getAddress() == 13) {
			if (con.getOtherNode(getHost()).getAddress() == 36 || con.getOtherNode(getHost()).getAddress() == 13) {
				System.out.printf("%d at time %d: changedConnection to %s\n", getHost().getAddress(), SimClock.getIntTime(), con.isUp()? "UP":"DOWN");
			}
		}
		 */
		super.changedConnection(con);
		DTNHost other = con.getOtherNode(getHost());
		Integer j = other.getAddress();
		if (con.isUp()) {
			// Exchange message IDs known to be delivered and delete them.
			updateKnownDeliveredMessages(((SEDUMRouter) other.getRouter())
					.getKnownDeliveredMessages());

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
								lastUpdatedTime.put(k, SimClock.getIntTime());
							}

						} else {
							durationUtilities.put(k, relayUtility);
							lastUpdatedTime.put(k, SimClock.getIntTime());
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
		/*
		boolean printStuff = (requester == 36 || requester == 13);
		if (printStuff) {
			System.out.printf("\tSTART TIME is %d\n", startTime);
		}
		 */
		Map<Integer, DurationUtility> updatedUtilities = new HashMap<Integer, DurationUtility>();
		for (Integer j: durationUtilities.keySet()) {
			if (j != requester) {
				/*
				if (printStuff) {
					System.out.printf("\tUtility for %d in %d was last updated at %d\n", j, getHost().getAddress(), lastUpdatedTime.get(j));
				}
        System.out.printf("\t%d has utility for %d as %s\n", getHost().getAddress(), j, durationUtilities.get(j).toString());
			  System.out.printf("\tUtility for %d in %d was last updated at %d\n", j, getHost().getAddress(), lastUpdatedTime.get(j));
				 */

				if (lastUpdatedTime.get(j) > startTime) {
					/*
					if (printStuff) {
						System.out.printf("\t%d is seeking duration utility for %d\n", getHost().getAddress(), j);
					}
					 */
					updatedUtilities.put(j, durationUtilities.get(j));
				}
			}
		}
		return updatedUtilities;
	}

	@Override
	protected int startTransfer(Message m, Connection con) {
		if (otherNodeIsAGoodCandidate(m, con)) {
			// Modify the number of replicas of this message, and annotate
			//  it if it should be a core replica or a non-core replica.
			Message replica = m.replicate();
			Integer numberOfReplicas = (Integer) m.getProperty("NUM_REPLICAS");
			if (numberOfReplicas != 0) {
				replica.updateProperty(
					"NUM_REPLICAS", 
					(int) Math.floor((numberOfReplicas-1.0)/2)
				);
			}
			
			// Determine what type of replica type this message should be:
			//  either a core replica or a non-core replica.
			annotateReplicaType(replica, con);


			int result = super.startTransfer(replica, con);

			// If the message was accepted, modify the number of replicas
			//  for the local message.
			if (result == RCV_OK) {
				if (numberOfReplicas != 0) {
					m.updateProperty(
						"NUM_REPLICAS",
						(int) (Math.ceil((numberOfReplicas-1.0)/2))+1
					);
				}
			}

			return result;
		} else {
			return DENIED;
		}
	}

	private void annotateReplicaType(Message m, Connection con) {
		//  If this node is the source of the message, and if
		//  the receiving node has the largest duration utility of all
		//  connected nodes, and if a core replica has not already been
		//  delivered, then this replica must be marked as a core replica.
		//  Otherwise, it should be a non-core replica.
		
		// If this node is not the source of the message, then the message
		//  is a non-core replica.
		if (getHost().getAddress() != m.getFrom().getAddress()) {
			m.updateProperty("IS_CORE_REPLICA", false);
			return;
		} else {
		
			// If the source node has already allocated a core replica, then
			//  this message is a non-core replica.
			if (((SEDUMRouter) m.getFrom().getRouter()).hasAllocatedCoreReplicaFor(m.getId())) {
				m.updateProperty("IS_CORE_REPLICA", false);
				return;
			}
		
			DurationUtility receiverUtility = (
				(SEDUMRouter) con.getOtherNode(getHost()).getRouter()
			).getUtilities().get(m.getTo());
			if (receiverUtility == null) {
				receiverUtility = new DirectDurationUtility(0.0);
			}
			
			// Iterate through all active connections and see if one has
			//  a higher utility than the node at the other end of the provided
			//  connection.
			for (Connection c: getConnections()) {
				DTNHost neighbor = c.getOtherNode(getHost());
				
				// Skip over this connection if it is the one provided in the
				//  parameter list.
				if (neighbor.getAddress() == con.getOtherNode(getHost()).getAddress()) {
					break;
				}
				
				// Test if there exists a connected node that has a higher
				//  duration utility.
				DurationUtility neighborUtility = ((SEDUMRouter) neighbor.getRouter()).getUtilities().get(m.getTo().getAddress());
				if (neighborUtility == null) {
					neighborUtility = new DirectDurationUtility(0.0);
				}
				if (receiverUtility.isSmallerThan(neighborUtility)) {
					m.updateProperty("IS_CORE_REPLICA", false);
					return;
				}
			}
			
			// If we've reached here, then:
			//  1. This router is the source.
			//  2. This router has not allocated a core replica.
			//  3. The node receiving this message has the largest
			//      utility of all other connected nodes.
			m.updateProperty("IS_CORE_REPLICA", true);
			coreReplicas.add(m.getId());
		}
	}

	private boolean hasAllocatedCoreReplicaFor(String id) {
		return coreReplicas.contains(id);
	}

	private boolean otherNodeIsAGoodCandidate(Message m, Connection con) {
		DTNHost other = con.getOtherNode(getHost());

		// If the other node is the destination host, return true by default.
		if (other.getAddress() == m.getTo().getAddress()) {
			return true;
		}

		if ((Integer) m.getProperty("NUM_REPLICAS") == 1) {
			return false;
		}

		DurationUtility localUtility = durationUtilities.get(
			m.getTo().getAddress()
		);
		
		if (localUtility == null) {
			localUtility = new DirectDurationUtility(0.0);
		}

		if (localUtility.isRelayedBy(other.getAddress())) {
			return true;
		}

		DurationUtility neighborUtility = ((SEDUMRouter) other.getRouter())
				.getUtilities().get(m.getTo().getAddress());
		if (neighborUtility == null) {
			neighborUtility = new DirectDurationUtility(0.0);
		}
		
		if (localUtility.isSmallerThan(neighborUtility)) {
			return true;
		}

		return false;
	}

	@Override
	public Message messageTransferred(String id, DTNHost from) {
		//eta.iteration_marker(SimClock.getIntTime());
		//eta.ping("messageTransferred");

		Message m = super.messageTransferred(id, from);
		if (m.getTo().getAddress() == getHost().getAddress()) {
			knownDeliveredMessages.put(id, m);
		}
		return m;
	}

	@Override
	public int receiveMessage(Message m, DTNHost from) {
		if (isTransferring()) {
			return TRY_LATER_BUSY; // only one connection at a time
		}

		if ( hasMessage(m.getId()) || isDeliveredMessage(m) ||
				super.isBlacklistedMessage(m.getId())) {
			return DENIED_OLD; // already seen this message -> reject it
		}

		if (m.getTtl() <= 0 && m.getTo() != getHost()) {
			/* TTL has expired and this host is not the final recipient */
			return DENIED_TTL; 
		}

		// If there is free space to fit in the message, then accept.
		if (getFreeBufferSize() >= m.getSize()) {
			return super.receiveMessage(m, from);
		}

		// If all messages in buffer are core-replicas
		//   Reject message
		if (allMessagesAreCoreReplicas()) {
			return MessageRouter.DENIED_LOW_RESOURCES;
		}

		// If receiving message is a non-core replica
		if ((Boolean) m.getProperty("IS_CORE_REPLICA") == false) {
			//   If no message has a utility lower than recipient
			//     Reject message
			//   Else
			//     Replace message(s) with lowest utility with M
			if (removeLowestsUtilityMessagesToFit(m)) {
				return super.receiveMessage(m, from);
			} else {
				return MessageRouter.DENIED_LOW_RESOURCES;
			}
		} else {
			// Else
			//   Replace message(s) with lowest utility with M
			removeLowestsUtilityMessagesToFit(m);
		}


		// seems OK, start receiving the message
		return super.receiveMessage(m, from);
	}

	private boolean allMessagesAreCoreReplicas() {
		for (Message m: getMessageCollection()) {
			if ((Boolean) m.getProperty("IS_CORE_REPLICA") == false) {
				return false;
			}
		}
		return true;
	}

	private boolean removeLowestsUtilityMessagesToFit(Message m) {
		return false;
	}

	@Override
	protected Message getNextMessageToRemove(boolean excludeMsgBeingSent) {
		Collection<Message> messages = this.getMessageCollection();
		Message oldest = null;
		for (Message m : messages) {

			if (excludeMsgBeingSent && isSending(m.getId())) {
				continue; // skip the message(s) that router is sending
			}

			if (oldest == null ) {
				oldest = m;
			}
			else if (oldest.getReceiveTime() > m.getReceiveTime()) {
				oldest = m;
			}
		}

		return oldest;
	}

	public Map<String, Message> getKnownDeliveredMessages() {
		return knownDeliveredMessages;
	}

	public void updateKnownDeliveredMessages(Map<String, Message> fromNeighbor) {
		// Merge the known delivered messages from the neighbor with this 
		//  router.
		knownDeliveredMessages.putAll(fromNeighbor);

		// Delete any messages in the local buffer that are in this list.
		for (Message m: getMessageCollection()) {
			if (fromNeighbor.containsKey(m.getId())) {
				deleteMessage(m.getId(), false);
			}
		}
	}

	@Override 
	public boolean createNewMessage(Message m) {
		m.addProperty("NUM_REPLICAS", NUM_REPLICAS);
		m.addProperty("IS_CORE_REPLICA", false);
		return super.createNewMessage(m);
	}
}
