package models

case class TableObject(database: String, tableName: String, typeInfo: String, pkNames:Array[String], dataInfo: String) extends Serializable
