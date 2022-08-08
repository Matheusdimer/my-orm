package com.dimer.myorm;

public interface Entity<I> {
    I getId();
    void setId(I id);
}
