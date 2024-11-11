package ru.chaplyginma.worker;

import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;

import java.util.Random;

public class Worker extends Thread {

    private static final String NAME_TEMPLATE = "Worker_%d";
    private static int id = 1;
    private final Manager manager;
    private final Random random = new Random();

    public Worker(Manager manager) {
        this.setName(NAME_TEMPLATE.formatted(id++));
        this.manager = manager;
    }

    @Override
    public void run() {
        MapTask mapTask;
        try {
            mapTask = manager.getNextMapTask(Thread.currentThread());
            while (mapTask != null) {
                System.out.println(getName() + ": starting map task on file " + mapTask.getFile());
                map(mapTask);
                manager.completeMapTask(mapTask, "MapTask: " + mapTask.getId() + " completed");

                mapTask = manager.getNextMapTask(Thread.currentThread());
            }
        } catch (InterruptedException e) {
            interrupt();
            System.out.println(getName() + " interrupted");
        }
        System.out.println(getName() + " finished");
    }

    private void map(MapTask mapTask) {
        System.out.println(getName() + ": processing file " + mapTask.getFile());
        try {
            Thread.sleep(100 + (long)random.nextInt(1500));
        } catch (InterruptedException e) {
            interrupt();
            System.out.println("map task interrupted");
        }
        System.out.println(getName() + " finis task " + mapTask.getId());
    }

}
