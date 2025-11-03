package com.sandip.concurrent;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sandip.concurrent.Main.Task;

public class TaskExecutorService implements Main.TaskExecutor {
	private final Logger log = LoggerFactory.getLogger(this.getClass().getName());
	private final ExecutorService executorService;
	private final BlockingQueue<TaskEntry<?>> tasksQueue = new LinkedBlockingQueue<>();
	private final Map<UUID, Lock> groupLocks = new ConcurrentHashMap<>();
	private final Thread schedulerThread;

	public TaskExecutorService(int maxConcurrency) {
		if (maxConcurrency <= 0) {
			int cores = Runtime.getRuntime().availableProcessors();
			maxConcurrency = Math.max(1, cores);
		}
		this.executorService = Executors.newFixedThreadPool(maxConcurrency);
		this.schedulerThread = new Thread(this::scheduleTasks, "task-scheduler");
		this.schedulerThread.setDaemon(true);
		this.schedulerThread.start();
	}

	public TaskExecutorService() {
		this(0);
	}

	@Override
	public <T> Future<T> submitTask(Task<T> task) {
		if (task == null) {
			throw new IllegalArgumentException("Task must not be null");
		}
		CompletableFuture<T> future = new CompletableFuture<>();
		TaskEntry<T> entry = new TaskEntry<>(task, future);
		tasksQueue.add(entry);
		return future;
	}

	private void scheduleTasks() {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				TaskEntry<?> entry = tasksQueue.take();
				if (entry == null) {
					continue;
				}
				executorService.submit(() -> runTask(entry));
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
	}

	@SuppressWarnings("unchecked")
	private void runTask(TaskEntry<?> entry) {
		UUID groupId = entry.task.taskGroup().groupUUID();
		Lock groupLock = groupLocks.computeIfAbsent(groupId, id -> new ReentrantLock(true));
		groupLock.lock();
		try {
			log.info(entry.task.taskName() + " submitted to executor after acquiruing the groupLock");
			//log.info("{} submitted to executor after acquiruing the groupLock {}", entry.task.taskName(), groupId);
			Object result = entry.task.taskAction().call();
			((CompletableFuture<Object>) entry.future).complete(result);
		} catch (Exception ex) {
			((CompletableFuture<Object>) entry.future).completeExceptionally(ex);
		} finally {
			log.info("{} released the groupLock {}", entry.task.taskName(), groupId);
			groupLock.unlock();
		}
	}

	private static class TaskEntry<T> {
		final Task<T> task;
		final CompletableFuture<T> future;

		TaskEntry(Task<T> task, CompletableFuture<T> future) {
			this.task = task;
			this.future = future;
		}
	}

	public void shutdown() {
		schedulerThread.interrupt();
		executorService.shutdown();
	}
}