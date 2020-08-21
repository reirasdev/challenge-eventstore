package net.intelie.challenges.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import net.intelie.challenges.Event;
import net.intelie.challenges.EventIterator;

public class EventIteratorImpl implements EventIterator {

	private final Iterator<Event> EVENTS_ITERATOR;

	public EventIteratorImpl(Collection<Event> events) {

		if (events != null)
			EVENTS_ITERATOR = events.iterator();
		else
			EVENTS_ITERATOR = new ArrayList<Event>().iterator();
	}

	@Override
	public boolean moveNext() {

		return EVENTS_ITERATOR.hasNext();
	}

	@Override
	public Event current() {

		if (!this.moveNext())
			throw new IllegalStateException();

		return EVENTS_ITERATOR.next();
	}

	@Override
	public void remove() {

		EVENTS_ITERATOR.remove();
	}

	@Override
	public void close() throws Exception {

	}

}
