package org.tresql

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.WeakHashMap
import scala.collection.JavaConverters._
import QueryParser._

/** Cache for parsed expressions */
trait Cache extends CacheBase[Exp]
trait CacheBase[E] {
  def get(tresql: String): Option[E]
  def put(tresql: String, expr: E)
  def size: Int = ???
}

/** Cache based on java concurrent hash map */
class SimpleCache(maxSize: Int) extends SimpleCacheBase[Exp](maxSize) with Cache
class SimpleCacheBase[E](maxSize: Int) extends CacheBase[E] {
  private val cache: java.util.Map[String, E] = new ConcurrentHashMap[String, E]

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: E) = {
    if (maxSize != -1 && cache.size >= maxSize) {
      cache.clear
      println(s"[WARN] Tresql cache cleared, size exceeded $maxSize")
    }
    cache.putIfAbsent(tresql, expr)
  }

  override def size = cache.size
  def exprs = cache.keySet

  override def toString = "Simple cache exprs: " + exprs + "\n" + "Cache size: " + size
}

/** Cache based on scala WeakHashMap */
class WeakHashCache extends WeakHashCacheBase[Exp] with Cache
class WeakHashCacheBase[E] extends CacheBase[E] {
  private val cache = java.util.Collections.synchronizedMap(new WeakHashMap[String, E] asJava)

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: E) = cache.put(tresql, expr)

  override def size = cache.size
  def exprs: java.util.Set[String] = cache.keySet

  override def toString = "WeakHashCache exprs: " + exprs + "\n" + "Cache size: " + size
}
