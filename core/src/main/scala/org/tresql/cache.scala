package org.tresql

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.WeakHashMap
import scala.collection.JavaConverters._
import QueryParser._

/** Cache for parsed expressions */
trait Cache {
  def get(tresql: String): Option[Exp]
  def put(tresql: String, expr: Exp)
  def size: Int = ???
}

/** Cache based on java concurrent hash map */
class SimpleCache(maxSize: Int) extends Cache {
  private val cache: java.util.Map[String, Exp] = new ConcurrentHashMap[String, Exp]

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: Exp) = {
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
class WeakHashCache extends Cache {
  private val cache = java.util.Collections.synchronizedMap(new WeakHashMap[String, Exp] asJava)

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: Exp) = cache.put(tresql, expr)

  override def size = cache.size
  def exprs: java.util.Set[String] = cache.keySet

  override def toString = "WeakHashCache exprs: " + exprs + "\n" + "Cache size: " + size
}
