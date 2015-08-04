package routing;

public class DurationUtility {
	protected Double utility;
	protected Integer relay;
	
	public boolean isSmallerThan(DurationUtility other) {
		return utility < other.utility;
	}
	
	public boolean isRelayedBy(Integer j) {
		return relay == j;
	}

	public DurationUtility updateFromOld(DurationUtility oldDurationUtility,
			Double weight) {
		
		Double updatedUtility = (weight * utility) + (1-weight) * oldDurationUtility.utility;
		if (relay == null) {
			return new DirectDurationUtility(updatedUtility);
		} else {
			return new IndirectDurationUtility(updatedUtility, relay);
		}
	}
	
	public Double getUtility() {
		return utility;
	}

	public String toString() {
		return utility.toString() + (relay == null ? "" : " through " + relay.toString());
	}
}
