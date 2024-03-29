package org.tresql

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.WeakHashMap
import scala.collection.JavaConverters._
import ast.Exp

/** Cache for parsed expressions */
trait Cache extends CacheBase[Exp]
trait CacheBase[E] {
  def get(tresql: String): Option[E]
  def put(tresql: String, expr: E): Unit
  def size: Int = ???
  def toMap: Map[String, E]
  def load(cache: Map[String, E]): Unit
  def clear(): Unit
}

/** Cache based on java concurrent hash map */
class SimpleCache(maxSize: Int, name: String = "Tresql cache") extends SimpleCacheBase[Exp](maxSize, name) with Cache
class SimpleCacheBase[E](val maxSize: Int, val name: String) extends CacheBase[E] {
  private val cache: java.util.Map[String, E] = new ConcurrentHashMap[String, E]

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: E) = {
    if (maxSize != -1 && cache.size >= maxSize) {
      cache.clear
      println(s"[WARN] $name cleared, size exceeded $maxSize")
    }
    cache.putIfAbsent(tresql, expr)
  }

  override def size = cache.size
  def exprs = cache.keySet
  def toMap: Map[String, E] = cache.asScala.toMap
  def load(cache: Map[String, E]) = this.cache.putAll(cache.asJava)
  def clear(): Unit = cache.clear()
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
  def toMap: Map[String, E] = cache.asScala.toMap
  def load(cache: Map[String, E]) = this.cache.putAll(cache.asJava)
  def clear(): Unit = cache.clear()
  override def toString = "WeakHashCache exprs: " + exprs + "\n" + "Cache size: " + size
}
