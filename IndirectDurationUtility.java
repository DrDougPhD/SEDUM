package routing;

public class IndirectDurationUtility extends DurationUtility {

	public IndirectDurationUtility(DurationUtility directUtility, DurationUtility relayUtility, Integer relay) {
		this.relay = relay;
		this.utility = directUtility.utility * relayUtility.utility;
	}

	public IndirectDurationUtility(Double utility, Integer relay) {
		this.relay = relay;
		this.utility = utility;
	}

}
