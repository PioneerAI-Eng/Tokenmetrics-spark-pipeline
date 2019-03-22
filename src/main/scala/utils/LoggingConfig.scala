package utils

import ch.qos.logback.classic.{Level, LoggerContext}
import org.slf4j.LoggerFactory

trait LoggingConfig {
  private val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  loggerContext.getLogger("org.mongodb.driver").setLevel(Level.ERROR)
}
