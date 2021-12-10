package org.apache.dolphinscheduler.common.function;

@FunctionalInterface
public interface TriConsumer<U, T, S> {

    /**
     * Applies this function to the given arguments.
     * @param <U>
     * @param <T>
     * @param <S>
     * @return the function result
     */
    void accept(U u, T t, S s);
}
