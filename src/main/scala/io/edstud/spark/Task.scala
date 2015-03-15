package io.edstud.spark

object Task extends Enumeration {
    type Task = Value
    val Regression, Classification  = Value
}