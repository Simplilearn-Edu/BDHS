package com.com.simplilearn.bigdata.spark

import java.sql.Timestamp

import java.util.Calendar

object TimeUtils {
  private val monthName = Array("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
  private val quaters = Array("1st Quarter", "2nd Quarter", "3rd Quarter", "4th Quarter")

  def getMonth(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.MONTH)
  }

  def getYear(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.YEAR)
  }

  def numberToMonthName(number: Int): String = monthName(number)

  def numberToQuarter(number: Int): String = quaters(number - 1)

  def getQuater(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    (cal.get(Calendar.MONTH) / 3) + 1
  }
}
