package com.cgcoder;

import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPoolExecutor1 {

	private Map<Integer, String> taskMap = new ConcurrentHashMap<>();
	private static AtomicInteger threadCounter = new AtomicInteger();
	private static AtomicInteger taskCounter = new AtomicInteger();
	
	public static void run() {
		ExecutorService executor = Executors.newCachedThreadPool(r -> {
			return new Thread(r, "thread-cachedpool-" + threadCounter.incrementAndGet());
		});
		
		new CustomThreadPoolExecutor1().runJobs(executor);
	}
	
	public void sendCommands(String command) {
		sendCommands(command, null);
	}
	
	public void sendCommands(String command, Integer taskId) {
		if (taskId == null) {
			for (Integer task : this.taskMap.keySet()) {
				taskMap.put(task, command);
			}
		}
		else {
			taskMap.put(taskId, command);
		}
	}
	
	private Integer getAsInt(String[] command, int index, Integer defaultValue) {
		if (command.length < index) return defaultValue;
		
		try {
			return Integer.parseInt(command[index]);
		}
		catch (Exception e) {
			return defaultValue;
		}
	}
	
	private String[] parseCommands(String command) {
		if (command == null) return new String[0];
		return Arrays.asList(command.split(" ")).stream()
				.map(String::trim)
				.filter(s -> s.length() > 0)
				.toArray((c) -> new String[c]);
	}
	
	public void runJobs(ExecutorService executor) {
		Scanner scanner = new Scanner(System.in);
		while(true) {
			System.out.print(" > ");
			String[] commands = parseCommands(scanner.nextLine().trim());
			String command = commands[0];
			
			if (command.equals("stats")) {
				printStats();
			}
			else if (command.equals("new")) {
				for (int i = 0; i < getAsInt(commands, 1, 1); i++) {
					Integer id = taskCounter.incrementAndGet();
					FakeInDefiniteTask task = new FakeInDefiniteTask(id, taskMap);
					taskMap.put(id, "");
					executor.submit(task);
				}
			}
			else {
				System.out.println("Sending command '" + command + "'");
				sendCommands(command, getAsInt(commands, 1, null));
			}
			
			if (command.equals("terminate")) {
				break;
			}
			
			try {
				Thread.sleep(500);
			}
			catch (Exception e) {}
		}
		System.out.println("Terminated!");
		executor.shutdown();
	}
	
	public void printStats() {
		System.out.println("Threads created so far " + threadCounter.get());
		System.out.println("Tasks created so far " + taskCounter.get());
	}
	
	public static class FakeInDefiniteTask implements Runnable {

		private Integer taskId;
		private Map<Integer, String> taskMap;
		
		public FakeInDefiniteTask(Integer id, Map<Integer, String> taskMap) {
			this.taskId = id;
			this.taskMap = taskMap;
		}
		
		@Override
		public void run() {
			System.out.println("start : " + taskId + " ** " + Thread.currentThread().getName() + " ** start");
			while (true) {
				String task = this.taskMap.get(taskId);
				
				if (task != null && task.trim().length() > 0) {
					task = task.trim();
					if ("ping".equals(task)) {
						System.out.println(taskId + " ** " + Thread.currentThread().getName() + " ** ping");
					}
					else if ("quit".equals(task)) {
						System.out.println(taskId + " ** " + Thread.currentThread().getName() + " ** quit");
						break;
					}
					this.taskMap.put(taskId, "");
				}
				
				try {
					Thread.sleep(100);
				}
				catch (Exception e) {
					break;
				}
			}
			System.out.println(taskId + " ** " + Thread.currentThread().getName() + " ** ended");
		}
		
	}
}
