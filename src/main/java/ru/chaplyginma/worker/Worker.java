package ru.chaplyginma.worker;

import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;

public class Worker extends Thread {

    private static final String NAME_TEMPLATE = "Worker_%d";
    private static int id = 1;
    private final Manager manager;

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
                manager.completeMapTask(mapTask);

                mapTask = manager.getNextMapTask(Thread.currentThread());
            }
        } catch (InterruptedException e) {
            interrupt();
            System.out.println(getName() + " interrupted");
        }
        System.out.println(getName() + " finished");
    }

}
