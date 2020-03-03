package com.vondeconsulting.utils

import org.apache.log4j.Logger

trait Logging {
  lazy val log: Logger = Logger.getLogger(this.getClass.getName)
}
