package ru.chaplyginma.worker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.task.MapTaskResult;
import ru.chaplyginma.task.ReduceTask;
import ru.chaplyginma.task.ReduceTaskResult;
import ru.chaplyginma.testdata.TestFiles;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class WorkerIntegrationTest {

    private static final String WORK_DIR = "test_tmp";

    @Captor
    ArgumentCaptor<MapTaskResult> mapTaskResultArgumentCaptor;

    @Captor
    ArgumentCaptor<ReduceTaskResult> reduceTaskResultArgumentCaptor;


    @Test
    @DisplayName("Test worker creating number of files for map task equal to reduce task number")
    void givenNReduceTasks_whenExecuteMapTask_thenNIntermediateFilesCreated() throws InterruptedException {
        String file = (String) TestFiles.makeTestFiles(WORK_DIR, 1).toArray()[0];
        int numReduceTasks = 4;

        MapTask mapTask = new MapTask(file, numReduceTasks);

        Manager mockManager = Mockito.mock(Manager.class);

        given(mockManager.getTask())
                .willReturn(mapTask);
        given(mockManager.isWorking())
                .willReturn(true)
                .willReturn(false);

        Worker worker = new Worker(mockManager, WORK_DIR, 1);
        worker.start();
        worker.join();

        await().atMost(100, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> !mockManager.isWorking());

        verify(mockManager).completeMapTask(any(), mapTaskResultArgumentCaptor.capture());
        MapTaskResult expectedResult = mapTaskResultArgumentCaptor.getValue();

        assertThat(expectedResult.files()).hasSize(numReduceTasks);
    }

    @Test
    @DisplayName("Test full worker cycle on a predefined file make specific content")
    void givenPredefinedFile_whenExecuteTasks_thenSpecificFileGenerated() throws InterruptedException {
        String file = TestFiles.createSimpleTestFile(WORK_DIR);
        int numReduceTasks = 4;

        ReduceTask.resetGenerator();
        MapTask mapTask = new MapTask(file, numReduceTasks);

        Set<KeyValue> expectedSet = new HashSet<>(Set.of(
                new KeyValue("one", "2"),
                new KeyValue("two", "2"),
                new KeyValue("three", "1"),
                new KeyValue("four", "1"),
                new KeyValue("five", "1"),
                new KeyValue("six", "1")
        ));

        String mapDir = "%s/Worker_1/0".formatted(WORK_DIR);
        ReduceTask reduceTask = new ReduceTask(Set.of(
                "%s/mr-0-0.txt".formatted(mapDir),
                "%s/mr-0-2.txt".formatted(mapDir)
        ));

        Manager mockManager = Mockito.mock(Manager.class);

        given(mockManager.getTask())
                .willReturn(mapTask)
                .willReturn(reduceTask);
        given(mockManager.isWorking())
                .willReturn(true)
                .willReturn(true)
                .willReturn(false);

        Worker worker = new Worker(mockManager, WORK_DIR, 1);
        worker.start();
        worker.join();

        await().atMost(100, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> !mockManager.isWorking());

        verify(mockManager).completeReduceTask(any(), reduceTaskResultArgumentCaptor.capture());
        ReduceTaskResult result = reduceTaskResultArgumentCaptor.getValue();

        assertThat(result.keyValueSet()).containsExactlyInAnyOrderElementsOf(expectedSet);
    }

}