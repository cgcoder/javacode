package com.cgcoder;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MicroBatchQueueExecutor {

	public static void run() {
		BlockingQueue<List<Integer>> taskQueue = new LinkedBlockingQueue<List<Integer>>();
		MicroBatcher<Integer, List<Integer>> microBatcher = new MicroBatcher<>(
				10, 
				Duration.ofSeconds(1),
				2,
				taskQueue,
				(input) -> input);
		
		ExecutorService poolExec = Executors.newFixedThreadPool(5);
		
		for (int i = 0; i < 5; i++) {
			poolExec.submit(new ListPrinter(taskQueue));
		}
		
		for (int i = 0; i < 1000; i++) {
			try {
				// Thread.sleep(Math.round(Math.random()*50));
			}
			catch (Exception e) {}
			boolean queued = microBatcher.offer((i+1));
			if (!queued) {
				System.out.println(i + " rejected.");
			}
		}
		
		try {
			poolExec.awaitTermination(15, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class ListPrinter implements Runnable {

		private BlockingQueue<List<Integer>> taskQueue;
		private static final AtomicInteger counter = new AtomicInteger();
		private final int id;
		
		
		public ListPrinter(BlockingQueue<List<Integer>> taskQueue) {
			super();
			this.taskQueue = taskQueue;
			this.id = counter.incrementAndGet();
		}



		@Override
		public void run() {
			while (true) {
				try {
					List<Integer> batch = taskQueue.poll(5000, TimeUnit.MILLISECONDS);
					if (batch == null) continue;
					
					Thread.sleep(5000);
					StringBuilder sb = new StringBuilder();
					sb.append("Task Id " + id + " -> ");
					sb.append(Thread.currentThread().getName() + " -> ");
					sb.append("[");
					for (Integer b : batch) {
						sb.append(b).append(" ");
					}
					sb.append("]");
					System.out.println(sb.toString());
				} 
				catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
			
		}
		
	}
	
}
