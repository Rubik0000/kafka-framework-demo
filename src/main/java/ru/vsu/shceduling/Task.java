package ru.vsu.shceduling;

public class Task implements Runnable {

    private final ScheduledTask scheduledTask;

    public Task(ScheduledTask scheduledTask) {
        this.scheduledTask = scheduledTask;
    }

    @Override
    public void run() {
        try {
            while (!scheduledTask.isCanceled()) {
                scheduledTask.execute();
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
