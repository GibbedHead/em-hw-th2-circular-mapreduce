package ru.chaplyginma.manager;

import org.instancio.Instancio;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.task.*;
import ru.chaplyginma.testdata.TestFiles;
import ru.chaplyginma.worker.Worker;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.instancio.Select.field;

class ManagerIntegrationTest {

    private static final String WORK_DIR = "test_tmp";
    private static final String RESULT_DIR = "%s/result".formatted(WORK_DIR);

    @Test
    @DisplayName("Test manager working state")
    void givenManagerWithWorker_whenIsWorkingBeforeAndAfterWorker_thenReturnTrueAndFalse() throws InterruptedException {
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, 5);

        Manager manager = new Manager(files, 5, RESULT_DIR);
        manager.start();

        assertThat(manager.isWorking()).isTrue();

        Worker worker = new Worker(manager, RESULT_DIR);
        worker.start();

        manager.join();
        worker.join();

        assertThat(manager.isWorking()).isFalse();
    }

    @Test
    @DisplayName("Test manager with tasks, when getTask() is called, then task should be non-null and an instance of MapTask")
    void givenNewManager_whenGetTaskFirstTime_thenTaskIsNotNullAndIsInstanceOfMapTask() throws InterruptedException {
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, 5);

        Manager manager = new Manager(files, 5, RESULT_DIR);
        manager.start();

        Task task = manager.getTask();

        assertThat(task).isNotNull().isInstanceOf(MapTask.class);
    }

    @Test
    @DisplayName("Test manager with 1 task, when getTask() is called second time, then null returned")
    void givenManagerWith1Task_whenGetTaskCalledSecondTime_thenReturnNull() throws InterruptedException {
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, 1);

        Manager manager = new Manager(files, 5, RESULT_DIR);
        manager.start();

        manager.getTask();
        Task task2 = manager.getTask();

        assertThat(task2).isNull();
    }

    @Test
    @DisplayName("Test task timeout. Get task, wait 500 ms, get same task")
    void givenManagerWithTasks_whenGetTaskAfterTimeout_thenReturnSameTask() throws InterruptedException {
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, 1);

        Manager manager = new Manager(files, 5, RESULT_DIR);
        manager.start();

        Task task1 = manager.getTask();

        await().timeout(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    Task task2 = manager.getTask();

                    assertThat(task2).isSameAs(task1);
                });
    }

    @Test
    @DisplayName("Test manager with 1 task, calling completeMapTask and then getTask should return ReduceTask")
    void givenManagerWith1Task_whenCompleteMapTask_thenGetTaskReturnReduceTask() throws InterruptedException {
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, 1);

        Manager manager = new Manager(files, 5, RESULT_DIR);
        manager.start();

        MapTask mapTask = (MapTask) manager.getTask();

        MapTaskResult mapTaskResult = Instancio.create(MapTaskResult.class);

        manager.completeMapTask(mapTask, mapTaskResult);

        Task reduceTask = manager.getTask();

        assertThat(reduceTask).isInstanceOf(ReduceTask.class);
    }

    @Test
    @DisplayName("Test manager with N task and all tasks get N times, complete N times and repeat, manager should finish")
    void givenManager_whenAllTasksCompleted_thenManagerIsNotWorking() throws InterruptedException {
        int taskCount = 4;
        int reduceTaskCount = 5;
        Set<String> files = TestFiles.makeTestFiles(WORK_DIR, taskCount);

        Manager manager = new Manager(files, reduceTaskCount, RESULT_DIR);
        manager.start();

        for (int i = 0; i < taskCount; i++) {
            MapTask task = (MapTask) manager.getTask();
            MapTaskResult mapTaskResult = Instancio.create(MapTaskResult.class);
            manager.completeMapTask(task, mapTaskResult);
        }

        for (int i = 0; i < reduceTaskCount; i++) {
            ReduceTask task = (ReduceTask) manager.getTask();
            ReduceTaskResult reduceTaskResult = Instancio.of(ReduceTaskResult.class)
                    .supply(
                            field(ReduceTaskResult::keyValueSet),
                            random -> Set.of(new KeyValue("word", String.valueOf(random.intRange(1, 10)))))
                    .create();

            manager.completeReduceTask(task, reduceTaskResult);
        }

        manager.join();
        assertThat(manager.isWorking()).isFalse();
    }
}