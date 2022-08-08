package com.dimer.myorm;

import java.util.List;

public interface RepositoryOperation<T extends Entity<I>, I> {
    List<T> findAll();

    T find(I id);

    T save(T entity);

    List<T> saveAll(List<T> entities);

    T update(T entity);

    List<T> updateAll(List<T> entities);

    boolean delete(T entity);

    boolean deleteById(I id);

    T findByColumn(String column, Object value);

    List<T> findAllByColumn(String column, Object value);

    boolean exists(I id);
}
