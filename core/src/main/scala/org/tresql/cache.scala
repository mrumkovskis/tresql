package org.tresql

import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap

/** Cache for parsed expressions */
trait Cache {
  def get(tresql: String): Option[Any]
  def put(tresql: String, expr: Any)
}

class WeakHashCache extends Cache {
  private val cache = new ConcurrentHashMap[String, Any](
    new WeakHashMap[String, Any]())
  
  def get(tresql: String) = Option(cache.get(tresql))
  def put(tresql: String, expr: Any) = cache.putIfAbsent(tresql, expr)
  
  def size = cache.size
  def exprs = cache.keySet
  
}