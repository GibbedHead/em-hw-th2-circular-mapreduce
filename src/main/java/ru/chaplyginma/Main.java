package ru.chaplyginma;

import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.util.FileUtil;
import ru.chaplyginma.worker.Worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        final int numWorkers = 10;
        final int numReduceTasks = 7;
        final String workDir = "result";

        FileUtil.clearDirectory(new File(workDir));

        try {
            Manager manager = new Manager(loadFileNames(), numReduceTasks);
            manager.start();

            List<Worker> workers = new ArrayList<>(numWorkers);

            for (int i = 0; i < numWorkers; i++) {
                workers.add(new Worker(manager, workDir));
            }

            workers.forEach(Thread::start);

            manager.join();
            for (Worker worker : workers) {
                worker.join();
            }
        } catch (IOException e) {
            System.out.println("Can't load files: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Threads interrupted: " + e.getMessage());
            throw new RuntimeException(e);
        }


    }

    private static Set<String> loadFileNames() throws IOException {
        return new LinkedHashSet<>(Files.readAllLines(Paths.get("file_list.txt")));
    }
}