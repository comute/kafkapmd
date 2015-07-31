package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
import io.confluent.streaming.Predicate;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by yasuhiro on 6/18/15.
 */
class KStreamBranch<K, V> implements Receiver {

  private final Predicate<K, V>[] predicates;
  final KStreamSource<K, V>[] branches;

  @SuppressWarnings("unchecked")
<<<<<<< HEAD
  KStreamBranch(Predicate<K, V>[] predicates, KStreamTopology topology) {
    this.predicates = Arrays.copyOf(predicates, predicates.length);
    this.branches = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, predicates.length);
    for (int i = 0; i < branches.length; i++) {
      branches[i] = new KStreamSource<>(null, topology);
=======
  KStreamBranch(Predicate<K, V>[] predicates, KStreamInitializer initializer) {
    this.predicates = Arrays.copyOf(predicates, predicates.length);
    this.branches = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, predicates.length);
    for (int i = 0; i < branches.length; i++) {
      branches[i] = new KStreamSource<>(null, initializer);
>>>>>>> new api model
    }
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
<<<<<<< HEAD
    for (KStreamSource<K, V> branch : branches) {
      branch.bind(context, metadata);
=======
    for (KStreamSource stream : branches) {
      stream.bind(context, metadata);
>>>>>>> new api model
    }
  }

  @SuppressWarnings("unchecked")
  @Override
<<<<<<< HEAD
  public void receive(Object key, Object value, long timestamp) {
    for (int i = 0; i < predicates.length; i++) {
      Predicate<K, V> predicate = predicates[i];
      if (predicate.apply((K)key, (V)value)) {
        branches[i].receive(key, value, timestamp);
        return;
      }
    }
  }

  @Override
  public void close() {
    for (KStreamSource<K, V> branch : branches) {
      branch.close();
=======
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    for (int i = 0; i < predicates.length; i++) {
      Predicate<K, V> predicate = predicates[i];
      if (predicate.apply((K)key, (V)value)) {
        branches[i].receive(key, value, timestamp, streamTime);
        return;
      }
>>>>>>> new api model
    }
    return;
  }

}
