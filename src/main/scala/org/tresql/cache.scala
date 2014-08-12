package org.tresql

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.WeakHashMap
import scala.collection.JavaConversions._

/** Cache for parsed expressions */
trait Cache {
  def get(tresql: String): Option[Any]
  def put(tresql: String, expr: Any)
}

/** Cache based on java concurrent hash map */
class SimpleCache extends Cache {
  private val cache = new ConcurrentHashMap[String, Any]

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: Any) = cache.putIfAbsent(tresql, expr)

  def size = cache.size
  def exprs = cache.keySet
  
  override def toString = "Simple cache exprs: " + exprs + "\n" + "Cache size: " + size
}

/** Cache based on scala WeakHashMap */
class WeakHashCache extends Cache {
  private val cache = java.util.Collections.synchronizedMap(new WeakHashMap[String, Any])

  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: Any) = cache.put(tresql, expr)

  def size = cache.size
  def exprs = cache.keySet
  
  override def toString = "WeakHashCache exprs: " + exprs + "\n" + "Cache size: " + size
}