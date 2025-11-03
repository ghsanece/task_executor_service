package com.sandip.concurrent;

import java.util.UUID;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorServiceTest {
	private static final Logger log = LoggerFactory.getLogger(TaskExecutorServiceTest.class.getName());

	public static void main(String[] args) throws Exception {

		TaskExecutorService executor = new TaskExecutorService(5);
		Main.TaskGroup groupA = new Main.TaskGroup(UUID.randomUUID());
		Main.TaskGroup groupB = new Main.TaskGroup(UUID.randomUUID());
		Main.TaskGroup groupC = new Main.TaskGroup(UUID.randomUUID());

		Main.Task<String> taskA1 = new Main.Task<>(UUID.randomUUID(), groupA, Main.TaskType.READ, "taskA1", () -> {
			log.info("taskA1 running");
			return "A1 completed";
		});

		Main.Task<String> taskA2 = new Main.Task<String>(UUID.randomUUID(), groupA, Main.TaskType.WRITE, "taskA2",() -> {
			log.info("taskA2 running");
			return "A2 completed";
		});

		Main.Task<String> taskA3 = new Main.Task<String>(UUID.randomUUID(), groupA, Main.TaskType.WRITE, "taskA3",() -> {
			log.info("taskA3 running");
			return "A3 completed";
		});

		Main.Task<String> taskB1 = new Main.Task<String>(UUID.randomUUID(), groupB, Main.TaskType.WRITE, "taskB1",() -> {
			log.info("taskB1 running");
			return "B1 completed";
		});
		
		Main.Task<String> taskB2 = new Main.Task<String>(UUID.randomUUID(), groupB, Main.TaskType.READ, "taskB2",() -> {
			log.info("taskB2 running");
			return "B2 completed";
		});
		
		Main.Task<String> taskC1 = new Main.Task<String>(UUID.randomUUID(), groupC, Main.TaskType.WRITE, "taskC1",() -> {
			log.info("taskC1 running");
			return "C1 completed";
		});
		
		Main.Task<String> taskC2 = new Main.Task<String>(UUID.randomUUID(), groupC, Main.TaskType.READ, "taskC2",() -> {
			log.info("taskC2 running");
			return "C2 completed";
		});

		Future<String> futureA1 = executor.submitTask(taskA1);
		Future<String> futureA2 = executor.submitTask(taskA2);
		Future<String> futureA3 = executor.submitTask(taskA3);
		Future<String> futureB1 = executor.submitTask(taskB1);
		Future<String> futureB2 = executor.submitTask(taskB2);
		Future<String> futureC1 = executor.submitTask(taskC1);
		Future<String> futureC2 = executor.submitTask(taskC2);
		log.info("Task A1 result: " + futureA1.get());
		log.info("Task A2 result: " + futureA2.get());
		log.info("Task A3 result: " + futureA3.get());
		log.info("Task B1 result: " + futureB1.get());
		log.info("Task B2 result: " + futureB2.get());
		log.info("Task C1 result: " + futureC1.get());
		log.info("Task C2 result: " + futureC2.get());
		executor.shutdown();
	}
}