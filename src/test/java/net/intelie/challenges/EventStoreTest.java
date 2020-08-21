package net.intelie.challenges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import net.intelie.challenges.impl.EventStoreImpl;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EventStoreTest {

	@Test
	public void test10_insert() {
		EventStore eventStore = EventStoreImpl.getInstance();
		eventStore.insert(new Event("type01", 100));

		Event queryEvent = eventStore.query("type01", 0, 101).current();
		assertEquals("type01", queryEvent.type());
		assertEquals(100, queryEvent.timestamp());
	}

	@Test
	public void test20_removeAll() {
		EventStore eventStore = EventStoreImpl.getInstance();
		eventStore.insert(new Event("type01", 101));
		eventStore.insert(new Event("type01", 102));
		eventStore.insert(new Event("type01", 103));

		EventIterator queryEventIt = eventStore.query("type01", 0, 104);
		Event currentEvent;
		int count = 0;

		while (queryEventIt.moveNext()) {
			currentEvent = queryEventIt.current();
			assertEquals("type01", currentEvent.type());
			count++;
		}

		assertEquals(4, count);
		eventStore.removeAll("type01");

		queryEventIt = eventStore.query("type01", 0, 104);
		assertFalse(queryEventIt.moveNext());
	}

	@Test
	public void test30_query() {
		EventStore eventStore = EventStoreImpl.getInstance();
		String type = "type0";
		long timestamp = 0;
		for (int i = 0; i < 10; i++) {

			for (int x = 0; x < 10; x++) {
				eventStore.insert(new Event(type + i, timestamp++));
			}

			timestamp = (timestamp - 10) + 100;
		}

		int count = 0;
		for (int i = 0; i < 10; i++) {
			EventIterator eventIterator = eventStore.query(type + i, 0, 1000);

			while (eventIterator.moveNext()) {
				assertEquals(type + i, eventIterator.current().type());
				count++;
			}

		}

		assertEquals(100, count);
	}

	@Test
	public void test31_query() {
		EventStore eventStore = EventStoreImpl.getInstance();
		int count = 0;

		EventIterator eventIterator = eventStore.query("type05", 500, 506);
		while (eventIterator.moveNext()) {
			Event currentEvent = eventIterator.current();
			assertEquals("type05", currentEvent.type());
			assertTrue(currentEvent.timestamp() >= 500 && currentEvent.timestamp() < 506);
			count++;
		}

		assertEquals(6, count);
	}
	
	@Test
	public void test32_query() {
		assertFalse(EventStoreImpl.getInstance().query(null, 0, 100).moveNext());
	}

	@Test
	public void test33_query() {
		assertFalse(EventStoreImpl.getInstance().query("xxxx", 0, 100).moveNext());
	}

	@Test
	public void test34_query() {
		assertFalse(EventStoreImpl.getInstance().query("type05", 0, -1).moveNext());
	}
	
	@Test
	public void test35_query() {
		assertFalse(EventStoreImpl.getInstance().query("type05", 100_000, 506).moveNext());
	}
	
	@Test
	public void test36_query() {
		assertFalse(EventStoreImpl.getInstance().query("type05", 100_000, -1).moveNext());
	}

	/*
	 * Intention here is to test concurrency adding new events for the same type by
	 * multiple threads at the same time.
	 * Remove and query operation will also be performed simultaneously.
	 * The final result must demonstrate that the storage was consistently updated 
	 * and the implementation has good performance not getting any lock due to high concurrency.
	 * The tests are done for 6 different event types at the same time.
	 * In the end, all of that must have the right number of events to pass the test.
	 */
	@Test
	public void test40_concurrency() throws Exception {

		// We will have 1.000 threads adding new events concurrently
		ExecutorService executorService = Executors.newFixedThreadPool(1_000);
		List<Callable<Boolean>> tasksList = new ArrayList<Callable<Boolean>>();
		long beforeStart = System.currentTimeMillis();
		long tenMinAheadTimestamp = System.currentTimeMillis() + 86_400_000;

		Callable<Boolean> task100 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type100", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task101 = () -> {
			EventStoreImpl.getInstance().query("type100", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task200 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type200", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task201 = () -> {
			EventStoreImpl.getInstance().query("type200", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task300 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type300", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task301 = () -> {
			EventStoreImpl.getInstance().query("type300", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task400 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type400", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task401 = () -> {
			EventStoreImpl.getInstance().query("type400", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task500 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type500", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task501 = () -> {
			EventStoreImpl.getInstance().query("type500", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task600 = () -> {
			EventStoreImpl.getInstance().insert(new Event("type600", System.currentTimeMillis()));
			return true;
		};
		
		Callable<Boolean> task601 = () -> {
			EventStoreImpl.getInstance().query("type600", beforeStart, tenMinAheadTimestamp);
			return true;
		};

		Callable<Boolean> task602 = () -> {
			EventStoreImpl.getInstance().removeAll("type600");
			return true;
		};
		

		/*
		 * Each task, for each event type, will be repeated 50.000 times concurrently.
		 * It means we should have 20.000 events for each type in the end.
		 */
		for (int i = 0; i < 20_000; i++) {
			tasksList.add(task100);
			tasksList.add(task101);
			tasksList.add(task200);
			tasksList.add(task201);
			tasksList.add(task300);
			tasksList.add(task301);
			tasksList.add(task400);
			tasksList.add(task401);
			tasksList.add(task500);
			tasksList.add(task501);
			tasksList.add(task600);
			tasksList.add(task601);
			tasksList.add(task602);
		}
					
		executorService.invokeAll(tasksList);
		executorService.shutdown();
		executorService.awaitTermination(1, TimeUnit.MINUTES);

		
		EventIterator eventIterator;
		Event currentEvent;
		int count = 0;

		for (int i = 100; i < 500; i += 100) {
			eventIterator = EventStoreImpl.getInstance().query("type" + i, beforeStart, tenMinAheadTimestamp);

			while (eventIterator.moveNext()) {
				currentEvent = eventIterator.current();

				assertEquals("type" + i, currentEvent.type());
				assertTrue(currentEvent.timestamp() >= beforeStart
						&& currentEvent.timestamp() < tenMinAheadTimestamp);

				count++;
			}

			//test if we have 20_000 events for each type
			assertEquals(20_000, count);
			count = 0;
		}
		
	}
}