package com.github.hekonsek.rxjava.pipes;

import io.reactivex.Observable;

public interface Source<T> {

    Observable<Event<T>> build();

}
