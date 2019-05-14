/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.common.AdminCommandFailedException
import kafka.utils.CommandDefaultOptions
import kafka.utils.CommandLineUtils
import kafka.utils.CoreUtils
import kafka.utils.Json
import kafka.utils.Logging
import joptsimple.util.EnumConverter
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.{AdminClient => JAdminClient}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.Utils
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

final object LeaderElectionCommand extends Logging {
  def main(args: Array[String]): Unit = {
    run(args, 30.second)
  }

  def run(args: Array[String], timeout: Duration): Unit = {
    val commandOptions = new LeaderElectionCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(
      commandOptions,
      "This tool attempts to elect a new leader from a set of topic partitions. The type of elections supported are preferred replicas and unclean replicas."
    )

    val electionType = commandOptions.options.valueOf(commandOptions.electionType)

    val jsonFileTopicPartitions = Option(commandOptions.options.valueOf(commandOptions.pathToJsonFile)).map { path  =>
      parseReplicaElectionData(Utils.readFileAsString(path))
    }

    val singleTopicPartition = (
      Option(commandOptions.options.valueOf(commandOptions.topic)),
      Option(commandOptions.options.valueOf(commandOptions.partition))
    ) match {
      case (Some(topic), Some(partition)) => Some(Set(new TopicPartition(topic, partition)))
      case _ => None
    }

    /* Note: No need to look at --all-topic-partitions as we want this to be None if it is use.
     * Jopt-Simple should be validating that this option required if the --topic and --path-to-json-file
     */
    val topicPartitions = jsonFileTopicPartitions.orElse(singleTopicPartition)

    val adminClient = {
      val props = Option(commandOptions.options.valueOf(commandOptions.adminClientConfig)).map { config =>
        Utils.loadProps(config)
      }.getOrElse(new Properties())

      props.setProperty(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        commandOptions.options.valueOf(commandOptions.bootstrapServer)
      )
      props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)

      JAdminClient.create(props)
    }

    try {
      electLeaders(adminClient, electionType, topicPartitions)
    } finally {
      adminClient.close()
    }
  }

  private[this] def parseReplicaElectionData(jsonString: String): Set[TopicPartition] = {
    Json.parseFull(jsonString) match {
      case Some(js) =>
        js.asJsonObject.get("partitions") match {
          case Some(partitionsList) =>
            val partitionsRaw = partitionsList.asJsonArray.iterator.map(_.asJsonObject)
            val partitions = partitionsRaw.map { p =>
              val topic = p("topic").to[String]
              val partition = p("partition").to[Int]
              new TopicPartition(topic, partition)
            }.toBuffer
            val duplicatePartitions = CoreUtils.duplicates(partitions)
            if (duplicatePartitions.nonEmpty) {
              throw new AdminOperationException(
                s"Replica election data contains duplicate partitions: ${duplicatePartitions.mkString(",")}"
              )
            }
            partitions.toSet
          case None => throw new AdminOperationException("Replica election data is missing \"partition\" field")
        }
      case None => throw new AdminOperationException("Replica election data is empty")
    }
  }

  private[this] def electLeaders(
    client: JAdminClient,
    electionType: ElectionType,
    topicPartitions: Option[Set[TopicPartition]]
  ): Unit = {
    val partitions = topicPartitions.map(_.asJava).getOrElse(null)
    debug(s"Calling AdminClient.electLeaders($electionType, $partitions)")
    val result = client.electLeaders(electionType, partitions)

    val attemptedPartitions = topicPartitions.getOrElse {
      try {
        result.partitions().get.asScala
      } catch {
          case e: ExecutionException =>
            val cause = e.getCause
            if (cause.isInstanceOf[TimeoutException]) {
              println("Timeout waiting for election results")
            }
            throw new AdminCommandFailedException("Timeout waiting for election results", cause)
          case e: Throwable =>
            println("Error while making request")
            e.printStackTrace()
            return
      }
    }

    val (failed, succeeded) = attemptedPartitions
      .map(topicPartition => topicPartition -> result.partitionResult(topicPartition))
      .partition { case (_, partitionResult) => completedExceptionally(partitionResult) }

    if (!succeeded.isEmpty) {
      val partitions = succeeded.map(_._1).mkString(", ")
      println(s"Successfully completed leader election ($electionType) for partitions $partitions")
    }

    if (!failed.isEmpty) {
      val rootException = new AdminCommandFailedException(s"${failed.size} replica(s) could not be elected")
      failed.foreach { case (topicPartition, future) =>
        val exception = Try(future.get()).fold(
          identity,
          _ => new AdminCommandFailedException("Exceptional future with no exception")
        )
        println(s"Error completing leader election ($electionType) for partition: $topicPartition: $exception")
        rootException.addSuppressed(exception)
      }
      throw rootException
    }
  }

  private[this] def completedExceptionally(future: KafkaFuture[_]): Boolean = {
    try {
      future.get()
      false
    } catch {
      case (_: Throwable) =>
        true
    }
  }
}

private final class LeaderElectionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  val bootstrapServer = parser
    .accepts(
      "bootstrap-server",
      "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .required
    .describedAs("host:port")
    .ofType(classOf[String])
  val adminClientConfig = parser
    .accepts(
      "admin.config",
      "Configuration properties files to pass to the admin client")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])

  val pathToJsonFile = parser
    .accepts(
      "path-to-json-file",
      "The JSON file with the list  of partition for which leader elections should be performed. This is an example format. \n{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\nNot allowed if --all-topic-partitions or --topic flags are specified.")
    .withRequiredArg
    .describedAs("Path to JSON file")
    .ofType(classOf[String])

  val topic = parser
    .accepts(
      "topic",
      "Name of topic for which to perform an election. Not allowed if --path-to-json-file or --all-topic-partitions is specified.")
    .availableUnless("path-to-json-file")
    .withRequiredArg
    .describedAs("topic name")
    .ofType(classOf[String])
  val partition = parser
    .accepts(
      "partition",
      "Partition id for which to perform an election. REQUIRED if --topic is specified.")
    .requiredIf("topic")
    .withRequiredArg
    .describedAs("partition id")
    .ofType(classOf[Integer])

  val allTopicPartitions = parser
    .accepts(
      "all-topic-partitions",
      "Perform election on all of the topic partitions. Not allowed if --topic or --path-to-json-file is specified.")
    .requiredUnless("path-to-json-file", "topic")

  val electionType = parser
    .accepts(
      "election-type",
      "Type of election to attempt. Possible values are \"preferred\" for preferred leader election or \"unclean\" for unclean leader election. REQUIRED.")
    .withRequiredArg
    .required
    .describedAs("election type")
    .withValuesConvertedBy(ElectionTypeConverter)

  options = parser.parse(args: _*)
}

final object ElectionTypeConverter extends EnumConverter[ElectionType](classOf[ElectionType]) { }
