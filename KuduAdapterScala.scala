package com.everis.tacaca.adapters

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.kudu.spark.kudu._

class KuduAdapterScala(val sparkContext: SparkContext, val hiveContext: HiveContext, val kuduMaster: String, kuduDatabaseDestination: String, kuduTableDestination: String) {

  val sc: SparkContext = sparkContext
  val hive: HiveContext = hiveContext
  val kuduContext = new KuduContext(kuduMaster, this.sc)
  var tableTarget: String = ""
  val table = "impala::" + kuduDatabaseDestination.concat(".".concat(kuduTableDestination))

  def kuduInsert(df: DataFrame): Unit = {
    if (kuduContext.tableExists(table)) {
      kuduContext.insertRows(df, table)
    } else {
      throw new RuntimeException("\n>>> Ocorreu um erro ao realizar uma ingestao do tipo INSERT no Kudu na tabela [" + table + "] \n")
    }
    print("\n\n[Ingestao realizada com sucesso]")
  }

  def kuduUpdate(df: DataFrame): Unit = {
    if (kuduContext.tableExists(table)) {
      kuduContext.updateRows(df, table)
    } else {
      throw new RuntimeException("\n>>> Ocorreu um erro ao realizar uma ingestao do tipo UPDATE no Kudu na tabela [" + table + "] \n")
    }
    print("\n\n[Ingestao realizada com sucesso]")
  }

  def kuduUpsert(df: DataFrame): Unit = {
    if (kuduContext.tableExists(table)) {
      kuduContext.upsertRows(df, table)
    } else {
      throw new RuntimeException("\n>>> Ocorreu um erro ao realizar uma ingestao do tipo UPSERT no Kudu na tabela [" + table + "] \n")
    }
    print("\n\n[Ingestao realizada com sucesso]")
  }

  def kuduDelete(df: DataFrame): Unit = {
    if (kuduContext.tableExists(table)) {
      kuduContext.deleteRows(df, table)
    } else {
      throw new RuntimeException("\n>>> Ocorreu um erro ao realizar uma ingestao do tipo DELETE no Kudu na tabela [" + table + "] \n")
    }
    print("\n\n[Delete realizado com sucesso]")
  }

  def kuduSelect(table_ : String): DataFrame = {
    tableTarget = "".concat("impala::" + table_)
    if (kuduContext.tableExists(tableTarget)) {
      val df = hiveContext.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableTarget)).kudu
      df
    } else {
      throw new RuntimeException("\n>>> Ocorreu um erro ao realizar uma ingestao do tipo DELETE no Kudu na tabela [" + table + "] \n")
    }
  }
}