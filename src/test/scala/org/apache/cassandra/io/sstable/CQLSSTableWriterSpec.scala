package org.apache.cassandra.io.sstable

import collection.JavaConversions._
import com.rhapsody.bi.scalatest.BIBaseSpec

class CQLSSTableWriterSpec extends BIBaseSpec {

  "CQLSSTableWriterExt" should {
    "Write insert/update into SSTable" in {
      val writer = CQLSSTableWriterExt.builder()
        .forTable(
          """
            | create table testing.bulk_load (
            |   key text,
            |   rank int,
            |   value text,
            |   primary key (key, rank)
            | ) with clustering order by (rank asc);
          """.stripMargin)
        .inDirectory("testing/bulk_load")
        .forUpdate("insert into testing.bulk_load (key, rank, value) values (?,?,?)")
        .forDeleteRow("delete from testing.bulk_load where key=? and rank=?")
        .forDeletePartition("delete from testing.bulk_load where key=?")
        .sorted()
        .build()

//      writer.addRow("guid-1", 0.asInstanceOf[Object], "alb.2001")
//      writer.addRow("guid-1", 1.asInstanceOf[Object], "alb.2002")
//      writer.addRow("guid-1", 2.asInstanceOf[Object], "alb.2003")
//
//      writer.deleteRow("guid-1", 3.asInstanceOf[Object])
      writer.deletePartition("guid-1")

      writer.close()
    }
    "Write 'delete a row' into SSTable" in {
      pending
    }
    "Write 'delete all of a partition' into SSTable" in {
      pending
    }
  }

}
