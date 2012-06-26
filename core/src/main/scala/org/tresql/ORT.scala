package org.tresql

object ORT {
  
  def insert(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {}
  def update(name:String, obj:Map[String, _])(implicit resources:Resources = Env):Any = {}
  def delete(name:String, id:Any):Any = {}
  def get(name:String):Any = {}

  private def insert_tresql(name:String, obj:Map[String, _], parent:String)(resources:Resources) = {
    val hasPk = obj.keys.exists(_ == resources.metaData.table(name).key.cols(0))
  }
  
  
}