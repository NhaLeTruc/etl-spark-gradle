package com.etl.config

import com.etl.core._
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

/**
 * YAML configuration parser.
 *
 * Parses pipeline configuration from YAML format.
 */
class YAMLConfigParser {

  private val yaml = new Yaml()

  /**
   * Parse YAML string into PipelineConfig.
   *
   * @param yamlContent YAML configuration string
   * @return Parsed PipelineConfig
   * @throws Exception if YAML is invalid or required fields are missing
   */
  def parse(yamlContent: String): PipelineConfig = {
    val data = yaml.load(yamlContent).asInstanceOf[java.util.Map[String, Object]]
    parseConfig(data.asScala.toMap)
  }

  /**
   * Parse YAML file into PipelineConfig.
   *
   * @param filePath Path to YAML file
   * @return Parsed PipelineConfig
   */
  def parseFile(filePath: String): PipelineConfig = {
    val source = scala.io.Source.fromFile(filePath)
    try {
      val yamlContent = source.mkString
      parse(yamlContent)
    } finally {
      source.close()
    }
  }

  /**
   * Parse configuration map into PipelineConfig.
   */
  private def parseConfig(data: Map[String, Object]): PipelineConfig = {
    val pipelineId = data.getOrElse("pipelineId", throw new IllegalArgumentException("Missing pipelineId")).toString

    val source = parseSourceConfig(
      data.getOrElse("source", throw new IllegalArgumentException("Missing source")).asInstanceOf[java.util.Map[String, Object]].asScala.toMap
    )

    val transformations = data.get("transformations") match {
      case Some(transformList: java.util.List[_]) =>
        transformList.asScala.map { t =>
          parseTransformationConfig(t.asInstanceOf[java.util.Map[String, Object]].asScala.toMap)
        }.toList
      case _ => List.empty
    }

    val sink = parseSinkConfig(
      data.getOrElse("sink", throw new IllegalArgumentException("Missing sink")).asInstanceOf[java.util.Map[String, Object]].asScala.toMap
    )

    val performance = data.get("performance") match {
      case Some(perfMap: java.util.Map[_, _]) =>
        parsePerformanceConfig(perfMap.asInstanceOf[java.util.Map[String, Object]].asScala.toMap)
      case _ => PerformanceConfig(shufflePartitions = 200, batchSize = 10000)
    }

    val quality = data.get("quality") match {
      case Some(qualMap: java.util.Map[_, _]) =>
        parseQualityConfig(qualMap.asInstanceOf[java.util.Map[String, Object]].asScala.toMap)
      case _ => QualityConfig(schemaValidation = false, nullChecks = List.empty, duplicateCheck = false, customRules = None)
    }

    PipelineConfig(
      pipelineId = pipelineId,
      source = source,
      transformations = transformations,
      sink = sink,
      performance = performance,
      quality = quality
    )
  }

  private def parseSourceConfig(data: Map[String, Object]): SourceConfig = {
    SourceConfig(
      `type` = data.getOrElse("type", throw new IllegalArgumentException("Missing source.type")).toString,
      credentialsPath = data.getOrElse("credentialsPath", throw new IllegalArgumentException("Missing source.credentialsPath")).toString,
      parameters = parseParameters(data.get("parameters"))
    )
  }

  private def parseSinkConfig(data: Map[String, Object]): SinkConfig = {
    SinkConfig(
      `type` = data.getOrElse("type", throw new IllegalArgumentException("Missing sink.type")).toString,
      credentialsPath = data.getOrElse("credentialsPath", throw new IllegalArgumentException("Missing sink.credentialsPath")).toString,
      writeMode = data.getOrElse("writeMode", "append").toString,
      parameters = parseParameters(data.get("parameters"))
    )
  }

  private def parseTransformationConfig(data: Map[String, Object]): TransformationConfig = {
    val aggregations = data.get("aggregations") match {
      case Some(aggList: java.util.List[_]) =>
        Some(aggList.asScala.map { agg =>
          val aggMap = agg.asInstanceOf[java.util.Map[String, Object]].asScala.toMap
          AggregateExpr(
            column = aggMap("column").toString,
            function = aggMap("function").toString,
            alias = aggMap("alias").toString
          )
        }.toList)
      case _ => None
    }

    TransformationConfig(
      `type` = data.getOrElse("type", throw new IllegalArgumentException("Missing transformation.type")).toString,
      parameters = parseParameters(data.get("parameters")),
      aggregations = aggregations
    )
  }

  private def parsePerformanceConfig(data: Map[String, Object]): PerformanceConfig = {
    PerformanceConfig(
      shufflePartitions = data.get("shufflePartitions").map(_.toString.toInt).getOrElse(200),
      batchSize = data.get("batchSize").map(_.toString.toInt).getOrElse(10000)
    )
  }

  private def parseQualityConfig(data: Map[String, Object]): QualityConfig = {
    val nullChecks = data.get("nullChecks") match {
      case Some(checkList: java.util.List[_]) =>
        checkList.asScala.map(_.toString).toList
      case _ => List.empty
    }

    QualityConfig(
      schemaValidation = data.get("schemaValidation").map(_.toString.toBoolean).getOrElse(false),
      nullChecks = nullChecks,
      duplicateCheck = data.get("duplicateCheck").map(_.toString.toBoolean).getOrElse(false),
      customRules = None
    )
  }

  private def parseParameters(paramData: Option[Object]): Map[String, String] = {
    paramData match {
      case Some(params: java.util.Map[_, _]) =>
        params.asInstanceOf[java.util.Map[String, Object]].asScala.toMap.map {
          case (k, v) => k -> v.toString
        }
      case _ => Map.empty
    }
  }
}
