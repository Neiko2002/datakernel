package io.datakernel.rpc.client.sender;

import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

abstract class RequestSenderToGroup implements RequestSender {
	private final List<RequestSender> allSubSenders;
	private final List<RequestSender> activeSubSenders;
	private boolean active;

	public RequestSenderToGroup(List<RequestSender> senders) {
		checkNotNull(senders);
		this.allSubSenders = senders;
		this.activeSubSenders = filterSenders(senders);
		this.active = activeSubSenders.size() > 0;
	}

	protected final List<RequestSender> getActiveSubSenders() {
		return activeSubSenders;
	}

	protected final List<RequestSender> getAllSubSenders() {
		return allSubSenders;
	}

	@Override
	public final boolean isActive() {
		return active;
	}

	/**
	 * Returns only active senders
	 * @param subSenders
	 * @return {@link List} of active senders
	 */
	private static List<RequestSender> filterSenders(List<RequestSender> subSenders) {
		List<RequestSender> activeSenders = new ArrayList<>();
		for (RequestSender subSender : subSenders) {
			if (subSender.isActive()) {
				activeSenders.add(subSender);
			}
		}
		return activeSenders;
	}
}