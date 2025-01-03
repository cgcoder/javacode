package com.cgcoder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

public class MicroBatcher<I, O> {
	Queue<? super O> outputQueue = null;
	int maxBatchSize = 0;
	long maxWaitForBatchInMilliSeconds = 0;
	int maxOutputBacklog;
	List<I> buffer;
	Function<List<I>, O> batchFunc;
	Timer waitTimer;
	
	public static <I> Function<List<? extends I>, List<? extends I>> getListBatcher() {
		return (input) -> input;
	}
	
	public MicroBatcher(
			int maxBatchSize, 
			Duration maxWaitForBatch, 
			int maxOutputBacklog,
			Queue<? super O> outputQueue,
			Function<List<I>, O> batchFunc) {
		
		this.maxBatchSize = maxBatchSize;
		this.buffer = new ArrayList<>(this.maxBatchSize);
		this.maxOutputBacklog = maxOutputBacklog;
		this.outputQueue = outputQueue;
		this.maxWaitForBatchInMilliSeconds = maxWaitForBatch.toMillis();
		this.batchFunc = batchFunc;
		
		this.resetTimer();
		// this.waitTimer.scheduleAt(, this.maxWaitForBatchInMilliSeconds);
	}
	
	public boolean offer(I input) {
		synchronized (this.buffer) {
			if (outputQueue.size() >= this.maxOutputBacklog) {
				return false;
			}
			this.buffer.add(input);
			if (this.buffer.size() == this.maxBatchSize) {
				batchAndMoveToOutputBuffer();
			}
			return true;
		}
	}
	
	private void batchAndMoveToOutputBuffer() {
		List<I> readyBuffer = null;
		synchronized (this.buffer) {
			readyBuffer = new LinkedList<>(this.buffer);
			buffer.clear();
			this.resetTimer();
		}
		
		O output = this.batchFunc.apply(readyBuffer);
		boolean outputBatchQueued = outputQueue.offer(output);
		if (!outputBatchQueued) {
			System.out.println("Batch Rejected!");
		}
	}
	
	private void resetTimer() {
		if (this.waitTimer != null) {
			this.waitTimer.purge();
			this.waitTimer.cancel();
		}
			
		this.waitTimer = new Timer();
		this.waitTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				MicroBatcher.this.batchAndMoveToOutputBuffer();
			}
		}, 
		this.maxWaitForBatchInMilliSeconds, this.maxWaitForBatchInMilliSeconds);
	}
	
}