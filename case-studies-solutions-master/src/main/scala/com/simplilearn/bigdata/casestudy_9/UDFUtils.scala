package com.simplilearn.bigdata.casestudy_9

import org.apache.spark.sql.functions._

object UDFUtils {

  val toBoolean = udf { (studentOnBBus: Integer) => {
      if(studentOnBBus > 0) {
        false
      }else {
        true
      }
    }
  }
}
