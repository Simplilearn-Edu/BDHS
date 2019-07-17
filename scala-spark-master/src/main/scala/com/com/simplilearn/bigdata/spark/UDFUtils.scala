package com.com.simplilearn.bigdata.spark

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object UDFUtils {

  val formatter = java.text.NumberFormat.getInstance()

  val toMonth = udf { (timestamp: Timestamp) => {
      TimeUtils.getMonth(timestamp)
    }
  }

  val toYear = udf { (timestamp: Timestamp) => {
      TimeUtils.getYear(timestamp)
    }
  }

  val toQuarter = udf { (timestamp: Timestamp) => {
      TimeUtils.getQuater(timestamp)
    }
  }


  val toMonthName = udf { (monthNumber: Integer) => {
     TimeUtils.numberToMonthName(monthNumber)
    }
  }

  val toQuarterName = udf { (quarterNum: Integer) => {
      TimeUtils.numberToQuarter(quarterNum)
    }
  }

  val doubleToString = udf { (totalAmount: Double) => {
      formatter.format(totalAmount)
    }
  }

  val calculateProfit = udf { (marginPercentage: Integer, transactionAmount: Double) => {
      transactionAmount * (marginPercentage / 100.0)
    }
  }
}
