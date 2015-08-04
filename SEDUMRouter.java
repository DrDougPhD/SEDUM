package routing;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
		//System.out.printf("%d at time %d: update\n", getHost().getAddress(), SimClock.getIntTime());
		super.update();

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

	/**
	 * Calculate the duration utility for the previous time epoch between this
	 * node and the node with address j. This calculation is based on Formula
	 * 10 in the paper.
	 * @param j The address of the neighboring node
	 * @return the duration utility between this node and the j node
	 */
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

	/**
	 * Calculate the new duration utility, from the old duration utility and
	 * the utility from the most recent time epoch, between this node and the
	 * node with address j. This calculation is based on Formula 11 in the
	 * paper.
	 * @param j The address of the neighboring node
	 * @param currentDurationUtility The duration utility of the most recent 
	 * time epoch between this node and j.
	 * @return the updated duration utility between this node and the j node
	 */
	private DurationUtility calculateUpdatedUtility(Integer j, DurationUtility currentDurationUtility) {
		DurationUtility oldDurationUtility = durationUtilities.get(j);
		return currentDurationUtility.updateFromOld(oldDurationUtility, WEIGHT_CONSTANT);
	}

	/**
	 * Determine if a new time epoch has passed.
	 * @return True if the current time is the start of a new time epoch.
	 * False otherwise.
	 */
	private boolean newTimeEpoch() {
		if (SimClock.getIntTime() == epochStart + EPOCH_DURATION) {
			epochStart = SimClock.getIntTime();
			return true;
		}

		return false;
	}

	/**
	 * Called when a connection state is altered. If a new connection is
	 * established, the nodes exchange which message IDs are known to be
	 * delivered and exchange and update utility tables.
	 * @param con The altered connection
	 */
	@Override
	public void changedConnection(Connection con) {
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
			).getUtilityFor(m.getTo().getAddress());
			
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
				DurationUtility neighborUtility = ((SEDUMRouter) neighbor.getRouter()).getUtilityFor(m.getTo().getAddress());
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

	/**
	 * Determine if a core replica for a message has been created by
	 * the source node of the message.
	 * 
	 * @param id ID of message in question.
	 * @return True if core replica of message has been created. False
	 * otherwise.
	 */
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

		DurationUtility localUtility = getUtilityFor(m.getTo().getAddress());
		if (localUtility.isRelayedBy(other.getAddress())) {
			return true;
		}

		DurationUtility neighborUtility = ((SEDUMRouter) other.getRouter())
				.getUtilityFor(m.getTo().getAddress());
		
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

	/**
	 * Determine if there exists a non-core replica in this node's buffer.
	 * @return True if all messages are core replicas, False if there exists
	 * at least one non-core replica.
	 */
	private boolean allMessagesAreCoreReplicas() {
		for (Message m: getMessageCollection()) {
			if ((Boolean) m.getProperty("IS_CORE_REPLICA") == false) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Remove enough low-utility messages from this node's buffer so that
	 * m can fit into the buffer.
	 * 
	 * <p>
	 * It is assumed that this node's buffer has at least one non-core replica.
	 * The set of non-core replicas in this node's buffer are subject to
	 * deletion based on the properties of m and the messages in the set.
	 * <p>
	 * 1. If the current free space of this node's buffer and the size of the
	 * set of non-core replicas is smaller than the size of m, then nothing
	 * will be done. m cannot be added to the buffer.
	 * <p>
	 * 2. If m is a core replica and there exists some non-core replicas in
	 * the node's buffer that, if deleted, would permit m to fit, then the
	 * non-core replicas with the smallest utilities will be deleted in order
	 * to make space for m.
	 * <p>
	 * 3. If m is a non-core replica, then there needs to be enough non-core
	 * replicas with utilities smaller than m in this node that can be deleted
	 * in order to fit. 
	 * 
	 * @param m A new message that cannot fit in the buffer.
	 */
	private boolean removeLowestsUtilityMessagesToFit(Message m) {
		int freeSpace = getFreeBufferSize();
		if (freeSpace >= m.getSize()) {
			return true;
		}
		
		// Build a set of non-core replicas.
		int sizeOfNonCoreReplicas = 0;
		PriorityQueue<Message> nonCoreReplicas = new PriorityQueue<Message>(
			getMessageCollection().size(),
			new Comparator<Message>() {
				@Override
				public int compare(Message m1, Message m2) {
					Integer m1_destination = m1.getTo().getAddress();
					Integer m2_destination = m2.getTo().getAddress();
					Double m1_utility = getUtilityFor(m1_destination).getUtility();
					Double m2_utility = getUtilityFor(m2_destination).getUtility();
					if (m1_utility == m2_utility) {
						// Older messages should be deleted first.
						return ((m1.getCreationTime() - m2.getCreationTime()) < 0 ? -1 : 1);
					} else {
						// Messages with lower utilities should be deleted first.
						return ((m1_utility - m2_utility) < 0 ? -1 : 1 );
					}
				}
			}
		);
		
		if ((Boolean) m.getProperty("IS_CORE_REPLICA")) {
			// Build up the non core replicas to include all non-core replicas.
			for (Message localMessage: getMessageCollection()) {
				boolean isNonCoreReplica = false == (Boolean) localMessage.getProperty("IS_CORE_REPLICA");
				if (isNonCoreReplica) {
					nonCoreReplicas.add(localMessage);
					sizeOfNonCoreReplicas += localMessage.getSize();
				}
			}
		} else {
			// With m being a non-core replica, only messages with smaller
			// utilities than that of m can be removed from the buffer.
			DurationUtility utility = getUtilityFor(m.getTo().getAddress());
			for (Message localMessage: getMessageCollection()) {
				boolean isNonCoreReplica = false == (
					(Boolean) localMessage.getProperty("IS_CORE_REPLICA")
				);
				boolean hasSmallerUtility = getUtilityFor(
					localMessage.getTo().getAddress()
				).isSmallerThan(utility);
				
				if (isNonCoreReplica && hasSmallerUtility) {
					nonCoreReplicas.add(localMessage);
					sizeOfNonCoreReplicas += localMessage.getSize();
				}
			}
		}
		
		// Can we remove enough messages to guarantee free space for the
		// new message?
		if (sizeOfNonCoreReplicas + freeSpace < m.getSize()) {
			return false;
		}
		
		// Remove messages one by one until enough space has been freed
		// from the local buffer.
		while (freeSpace < m.getSize()) {
			Message toRemove = nonCoreReplicas.poll();
			freeSpace += toRemove.getSize();
			deleteMessage(toRemove.getId(), true);
		}
		return true;
	}

	protected DurationUtility getUtilityFor(Integer j) {
		if (durationUtilities.containsKey(j)) {
			return durationUtilities.get(j);
		} else {
			return new DirectDurationUtility(0.0);
		}
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

	/**
	 * Update the local node's set of known delivered messages based on a 
	 * neighbor's known set, and delete any messages that are now known to
	 * have been successfully delivered.
	 * 
	 * @param fromNeighbor The set of known delivered messages from this
	 * node's neighbor.
	 */
	protected void updateKnownDeliveredMessages(Map<String, Message> fromNeighbor) {
		// Merge the known delivered messages from the neighbor with this 
		//  router.
		knownDeliveredMessages.putAll(fromNeighbor);

		// Delete any messages in the local buffer that are in this list.
		Collection<Message> copy = new ArrayList<Message>(getMessageCollection());
		for (Message m: copy) {
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
	
	public Map<String, Message> getKnownDeliveredMessages() {
		return knownDeliveredMessages;
	}
}
