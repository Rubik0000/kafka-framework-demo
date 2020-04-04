package ru.vsu.shceduling;

public interface ScheduledTask {

    void execute();

    boolean isCanceled();
}
