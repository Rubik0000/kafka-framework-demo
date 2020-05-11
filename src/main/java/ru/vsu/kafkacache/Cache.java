package ru.vsu.kafkacache;

import java.util.Map;

public interface Cache<K, V> extends Map<K, V>, AutoCloseable {

    void resync();
}
