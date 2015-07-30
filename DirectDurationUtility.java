package routing;

public class DirectDurationUtility extends DurationUtility {

	public DirectDurationUtility(Integer cumulativeDuration, Integer epochDuration) {
		utility = (double) cumulativeDuration / epochDuration;
		relay = null;
	}

	public DirectDurationUtility(Double utility) {
		this.utility = utility;
		relay = null;
	}

}
