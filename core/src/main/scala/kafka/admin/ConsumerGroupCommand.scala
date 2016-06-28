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

import joptsimple.{OptionParser, OptionSpec}
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{TopicAndPartition, _}
import kafka.consumer.SimpleConsumer
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}

object ConsumerGroupCommand {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)
    val consumerGroupOutputWriter = new ConsumerGroupOutputWriter

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "List all consumer groups, describe a consumer group, or delete consumer group info.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete")

    opts.checkArgs()

    val consumerGroupService = {
      if (opts.options.has(opts.newConsumerOpt)) new KafkaConsumerGroupService(opts, consumerGroupOutputWriter)
      else new ZkConsumerGroupService(opts, consumerGroupOutputWriter)
    }

    try {
      if (opts.options.has(opts.listOpt))
        consumerGroupService.list()
      else if (opts.options.has(opts.describeOpt))
        consumerGroupService.describe()
      else if (opts.options.has(opts.deleteOpt)) {
        consumerGroupService match {
          case service: ZkConsumerGroupService => service.delete()
          case _ => throw new IllegalStateException(s"delete is not supported for $consumerGroupService")
        }
      }
    } catch {
      case e: Throwable =>
        println("Error while executing consumer group command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      consumerGroupService.close()
    }
  }

  case class ConsumerGroupAssignment(group: String, topicOpt: Option[String], partitionOpt: Option[Int], offsetOpt: Option[Long],
                                     lagOpt: Option[Long], memberIdOpt: Option[String], memberHostOpt: Option[String],
                                     clientIdOpt: Option[String], logEndOffset: Option[Long])

  trait OutputWriter {
    def printError(msg: String)
    def printAssignmentHeader()
    protected def printAssignmentRow(assignmentRow: ConsumerGroupAssignment)
    def printAssignmentRows(assignmentRows: Array[ConsumerGroupAssignment]): Unit = {
      assignmentRows.foreach { row => printAssignmentRow(row) }
    }
  }

  class ConsumerGroupOutputWriter extends OutputWriter {

    def printError(msg: String): Unit = println("Error: " + msg)

    def printAssignmentHeader(): Unit =
      println("%-30s %-30s %-10s %-15s %-15s %-10s %-50s %-30s %s".format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "MEMBER-ID", "MEMBER-HOST", "CLIENT-ID"))

    def printAssignmentRow(assignmentRow: ConsumerGroupAssignment): Unit =
     println("%-30s %-30s %-10s %-15s %-15s %-10s %-50s %-30s %s".format(assignmentRow.group, assignmentRow.topicOpt.getOrElse(""),
         assignmentRow.partitionOpt.getOrElse(""), assignmentRow.offsetOpt.getOrElse(""), assignmentRow.logEndOffset.getOrElse(""),
         assignmentRow.lagOpt.getOrElse(""), assignmentRow.memberIdOpt.getOrElse("none"), assignmentRow.memberHostOpt.getOrElse(""),
         assignmentRow.clientIdOpt.getOrElse("")))
  }

  trait ConsumerGroupService {

    def list(): Unit

    def describe() {
      describeGroup(opts.options.valueOf(opts.groupOpt)) match {
        case None =>
          consumerGroupOutputWriter.printError(s"The consumer group '${opts.groupOpt}' does not exist")
        case Some(results) =>
          consumerGroupOutputWriter.printAssignmentHeader()
          consumerGroupOutputWriter.printAssignmentRows(results)
          //printDescribeHeader()
          //printDescribeGroupResults(results)
      }
    }

    def close(): Unit

    protected def consumerGroupOutputWriter: OutputWriter

    protected def opts: ConsumerGroupCommandOptions

    protected def getLogEndOffset(topicAndPartition: TopicAndPartition): LogEndOffsetResult

    protected def describeGroup(group: String): Option[Array[ConsumerGroupAssignment]]

    protected def describeMemberTopicPartition(group: String,
                                               topicPartitions: Seq[TopicAndPartition],
                                               getPartitionOffset: TopicAndPartition => Option[Long],
                                               memberIdOpt: Option[String],
                                               memberHostOpt: Option[String],
                                               clientIdOpt: Option[String]): Array[ConsumerGroupAssignment] = {
      if (topicPartitions.isEmpty)
        Array[ConsumerGroupAssignment](getDescribeResult(group, None, None, None, memberIdOpt, memberHostOpt, clientIdOpt, None))
      else {
        var assignmentRows: Array[ConsumerGroupAssignment] = Array()
        topicPartitions
          .sortBy { case topicPartition => topicPartition.partition }
          .foreach { topicPartition =>
            assignmentRows = assignmentRows :+ describePartition(group, topicPartition.topic, topicPartition.partition, getPartitionOffset(topicPartition),
              memberIdOpt, memberHostOpt, clientIdOpt)
          }
        assignmentRows
      }
    }

    protected def printDescribeHeader() {
      consumerGroupOutputWriter.printAssignmentHeader();
    }
    
    protected def printDescribeGroupResults(assignmentRows: Array[ConsumerGroupAssignment]) = {
      consumerGroupOutputWriter.printAssignmentRows(assignmentRows)
    }
    
    protected def getDescribeResult(group: String,
                                    topicOpt: Option[String],
                                    partitionOpt: Option[Int],
                                    offsetOpt: Option[Long],
                                    memberIdOpt: Option[String],
                                    memberHostOpt: Option[String],
                                    clientIdOpt: Option[String],
                                    logEndOffsetOpt: Option[Long]): ConsumerGroupAssignment = {
        val lag = offsetOpt.filter(_ != -1).flatMap(offset => logEndOffsetOpt.map(_ - offset))
        new ConsumerGroupAssignment(group, topicOpt, partitionOpt, offsetOpt, lag, memberIdOpt, memberHostOpt, clientIdOpt, logEndOffsetOpt)
        //consumerGroupOutputWriter.printAssignment(group, topic, partition, offsetOpt, lag, memberIdOpt, memberHostOpt, clientIdOpt, logEndOffset)
    }

    private def describePartition(group: String,
                                  topic: String,
                                  partition: Int,
                                  offsetOpt: Option[Long],
                                  memberIdOpt: Option[String],
                                  memberHostOpt: Option[String],
                                  clientIdOpt: Option[String]): ConsumerGroupAssignment = {
      def getDescribePartitionResult(logEndOffsetOpt: Option[Long]): ConsumerGroupAssignment =
        getDescribeResult(group, Option(topic), Option(partition), offsetOpt, memberIdOpt, memberHostOpt, clientIdOpt, logEndOffsetOpt)
      
      getLogEndOffset(new TopicAndPartition(topic, partition)) match {
        case LogEndOffsetResult.LogEndOffset(logEndOffset) =>
          val a = getDescribePartitionResult(Some(logEndOffset))
          return a
        case LogEndOffsetResult.Unknown => getDescribePartitionResult(None)
        case LogEndOffsetResult.Ignore => null
      }
    }

  }

  class ZkConsumerGroupService(val opts: ConsumerGroupCommandOptions, val consumerGroupOutputWriter: OutputWriter) extends ConsumerGroupService {

    private val zkUtils = {
      val zkUrl = opts.options.valueOf(opts.zkConnectOpt)
      ZkUtils(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    }

    def close() {
      zkUtils.close()
    }

    def list() {
      zkUtils.getConsumerGroups().foreach(println)
    }

    def delete() {
      if (opts.options.has(opts.groupOpt) && opts.options.has(opts.topicOpt))
        deleteForTopic()
      else if (opts.options.has(opts.groupOpt))
        deleteForGroup()
      else if (opts.options.has(opts.topicOpt))
        deleteAllForTopic()
    }

    protected def describeGroup(group: String): Option[Array[ConsumerGroupAssignment]] = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      val channelSocketTimeoutMs = props.getProperty("channelSocketTimeoutMs", "600").toInt
      val channelRetryBackoffMs = props.getProperty("channelRetryBackoffMsOpt", "300").toInt
      if (!zkUtils.getConsumerGroups().contains(group))
        return None

      val topics = zkUtils.getTopicsByConsumerGroup(group)
      //if (topics.isEmpty) {
      //  consumerGroupOutputWriter.printError(s"No topic available for consumer group '$group'")
      //  return None
      //}

      var assignmentRows: Array[ConsumerGroupAssignment] = Array()

      val topicPartitions = getTopicsPartitions(topics)
      var groupMemberIds = zkUtils.getConsumersInGroup(group)

      // mapping of topic partition -> ephemeral owner
      val eOwnerByTopicPartition = mutable.Map[TopicAndPartition, Long]()
      topicPartitions.foreach { topicPartition =>
        val data = zkUtils.readDataMaybeNull(new ZKGroupTopicDirs(group, topicPartition.topic).consumerOwnerDir + "/" + topicPartition.partition)
        val owner = data._1
        if (!owner.isEmpty) {
          val eOwner = data._2.getEphemeralOwner()
          eOwnerByTopicPartition.put(topicPartition, eOwner)
        }
      }

      // mapping of ephemeral owner -> group member id
      val memberIdByEOwner = groupMemberIds.flatMap { memberId =>
        val eOwner = zkUtils.readDataMaybeNull(new ZKGroupDirs(group).consumerGroupDir + "/ids/" + memberId)._2.getEphemeralOwner()
        Some(eOwner).map { eOwner => eOwner -> memberId }
      }.toMap

      // mapping of topic partition -> member id
      val memberIdByTopicPartition = eOwnerByTopicPartition.flatMap { case (topicPartition, eOwner) =>
        val memberId = memberIdByEOwner.getOrElse(eOwner, "")
        Some(memberId.toString()).map { memberId => topicPartition -> memberId }
      }.toMap

      // mapping of member id -> list of topic partitions
      val memberTopicPartitions = memberIdByTopicPartition groupBy{_._2} map {
        case (key, value) => (key, value.unzip._1.toArray) }

      // mapping of member id -> list of subscribed topics
      val topicsByMemberId = zkUtils.getTopicsPerMemberId(group, true)

      topicPartitions.foreach { topicPartition =>
        val partitionOffsets = getPartitionOffsets(group, List(topicPartition), channelSocketTimeoutMs, channelRetryBackoffMs)
        val memberId = memberIdByTopicPartition.get(topicPartition)
        // since member id is repeated in client id, leave member host and client id empty
        assignmentRows = assignmentRows ++ describeMemberTopicPartition(group, List(topicPartition), partitionOffsets.get, memberId, None, None)
        if (!memberId.isEmpty) {
          groupMemberIds = groupMemberIds.filterNot { _ == memberId.get }
        }
      }

      groupMemberIds.sortBy { - memberTopicPartitions.get(_).size }.foreach { memberId =>
        topicsByMemberId(memberId).foreach { topic =>
          // since members with no topic partitions are processed here, we pass empty for topic partitions and offsets
          // since member id is repeated in client id, leave member host and client id empty
          assignmentRows = assignmentRows ++ describeMemberTopicPartition(group, Array[TopicAndPartition](), Map[TopicAndPartition, Option[Long]](), Some(memberId), None, None)
        }
      }

      Some(assignmentRows)
    }

    private def getTopicsPartitions(topics: Seq[String]): Seq[TopicAndPartition] = {
      val topicPartitionMap = zkUtils.getPartitionsForTopics(topics)
      var topicPartitions: Seq[TopicAndPartition] = Seq.empty
      topics.foreach { topic =>
        val partitions = topicPartitionMap.getOrElse(topic, Seq.empty)
        topicPartitions = topicPartitions ++ partitions.map(TopicAndPartition(topic, _))
      }
      topicPartitions
    }

    protected def getLogEndOffset(topicAndPartition: TopicAndPartition): LogEndOffsetResult = {
      zkUtils.getLeaderForPartition(topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(-1) => LogEndOffsetResult.Unknown
        case Some(brokerId) =>
          getZkConsumer(brokerId).map { consumer =>
            val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
            val logEndOffset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
            consumer.close()
            LogEndOffsetResult.LogEndOffset(logEndOffset)
          }.getOrElse(LogEndOffsetResult.Ignore)
        case None =>
          println(s"No broker for partition ${topicAndPartition}")
          LogEndOffsetResult.Ignore
      }
    }

    private def getPartitionOffsets(group: String,
                                    topicPartitions: Seq[TopicAndPartition],
                                    channelSocketTimeoutMs: Int,
                                    channelRetryBackoffMs: Int): Map[TopicAndPartition, Long] = {
      val offsetMap = mutable.Map[TopicAndPartition, Long]()
      val channel = ClientUtils.channelToOffsetManager(group, zkUtils, channelSocketTimeoutMs, channelRetryBackoffMs)
      channel.send(OffsetFetchRequest(group, topicPartitions))
      val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())

      offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
        if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
          val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
          // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
          // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
          try {
            val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1.toLong
            offsetMap.put(topicAndPartition, offset)
          } catch {
            case z: ZkNoNodeException =>
              println("Could not fetch offset from zookeeper for group %s partition %s due to missing offset data in zookeeper."
                .format(group, topicAndPartition))
          }
        }
        else if (offsetAndMetadata.error == Errors.NONE.code)
          offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
        else
          println("Could not fetch offset from kafka for group %s partition %s due to %s."
            .format(group, topicAndPartition, Errors.forCode(offsetAndMetadata.error).exception))
      }
      channel.disconnect()
      offsetMap.toMap
    }

    private def deleteForGroup() {
      val groups = opts.options.valuesOf(opts.groupOpt)
      groups.asScala.foreach { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInZK(zkUtils, group))
            println("Deleted all consumer group information for group %s in zookeeper.".format(group))
          else
            println("Delete for group %s failed because its consumers are still active.".format(group))
        }
        catch {
          case e: ZkNoNodeException =>
            println("Delete for group %s failed because group does not exist.".format(group))
        }
      }
    }

    private def deleteForTopic() {
      val groups = opts.options.valuesOf(opts.groupOpt)
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      groups.asScala.foreach { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic))
            println("Deleted consumer group information for group %s topic %s in zookeeper.".format(group, topic))
          else
            println("Delete for group %s topic %s failed because its consumers are still active.".format(group, topic))
        }
        catch {
          case e: ZkNoNodeException =>
            println("Delete for group %s topic %s failed because group does not exist.".format(group, topic))
        }
      }
    }

    private def deleteAllForTopic() {
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic)
      println("Deleted consumer group information for all inactive consumer groups for topic %s in zookeeper.".format(topic))
    }

    private def getZkConsumer(brokerId: Int): Option[SimpleConsumer] = {
      try {
        zkUtils.getBrokerInfo(brokerId)
          .map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT))
          .map(endPoint => new SimpleConsumer(endPoint.host, endPoint.port, 10000, 100000, "ConsumerGroupCommand"))
          .orElse(throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId)))
      } catch {
        case t: Throwable =>
          println("Could not parse broker info due to " + t.getMessage)
          None
      }
    }

  }

  class KafkaConsumerGroupService(val opts: ConsumerGroupCommandOptions, val consumerGroupOutputWriter: OutputWriter) extends ConsumerGroupService {

    private val adminClient = createAdminClient()

    // `consumer` is only needed for `describe`, so we instantiate it lazily
    private var consumer: KafkaConsumer[String, String] = null

    def list() {
      adminClient.listAllConsumerGroupsFlattened().foreach(x => println(x.groupId))
    }

    protected def describeGroup(group: String): Option[Array[ConsumerGroupAssignment]] = {
      adminClient.describeConsumerGroup(group) match {
        case None =>
          consumerGroupOutputWriter.printError(s"Consumer group `$group` does not exist.")
          None
        case Some(consumerSummaries) =>
          if (consumerSummaries.isEmpty) {
            consumerGroupOutputWriter.printError(s"Consumer group `$group` is rebalancing.")
            None
          }
          else {
            val consumer = getConsumer()
            var assignmentRows = Array[ConsumerGroupAssignment]()
            consumerSummaries.foreach { consumerSummary =>
              val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
              val partitionOffsets = topicPartitions.flatMap { topicPartition =>
                Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata =>
                  topicPartition -> offsetAndMetadata.offset
                }
              }.toMap
              assignmentRows = assignmentRows ++ describeMemberTopicPartition(group, topicPartitions, partitionOffsets.get,
                Some(s"${consumerSummary.memberId}"), Some(s"${consumerSummary.clientHost}"), Some(s"${consumerSummary.clientId}"))
            }
            Some(assignmentRows)
          }
      }
    }

    protected def getLogEndOffset(topicAndPartition: TopicAndPartition): LogEndOffsetResult = {
      val consumer = getConsumer()
      val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
      consumer.assign(List(topicPartition).asJava)
      consumer.seekToEnd(List(topicPartition).asJava)
      val logEndOffset = consumer.position(topicPartition)
      LogEndOffsetResult.LogEndOffset(logEndOffset)
    }

    def close() {
      adminClient.close()
      if (consumer != null) consumer.close()
    }

    private def createAdminClient(): AdminClient = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      AdminClient.create(props)
    }

    private def getConsumer() = {
      if (consumer == null)
        consumer = createNewConsumer()
      consumer
    }

    private def createNewConsumer(): KafkaConsumer[String, String] = {
      val properties = new Properties()
      val deserializer = (new StringDeserializer).getClass.getName
      val brokerUrl = opts.options.valueOf(opts.bootstrapServerOpt)
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, opts.options.valueOf(opts.groupOpt))
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
      if (opts.options.has(opts.commandConfigOpt)) properties.putAll(Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)))

      new KafkaConsumer(properties)
    }

  }

  sealed trait LogEndOffsetResult

  object LogEndOffsetResult {
    case class LogEndOffset(value: Long) extends LogEndOffsetResult
    case object Unknown extends LogEndOffsetResult
    case object Ignore extends LogEndOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED (unless new-consumer is used): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over."
    val BootstrapServerDoc = "REQUIRED (only when using new-consumer): The server to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted."
    val ListDoc = "List all consumer groups."
    val DescribeDoc = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group."
    val nl = System.getProperty("line.separator")
    val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2" + nl +
      "Pass in groups with a single topic to just delete the given topic's partition offsets and ownership " +
      "information for the given consumer groups. For instance --group g1 --group g2 --topic t1" + nl +
      "Pass in just a topic to delete the given topic's partition offsets and ownership information " +
      "for every consumer group. For instance --topic t1" + nl +
      "WARNING: Group deletion only works for old ZK-based consumer groups, and one has to use it carefully to only delete groups that are not active."
    val NewConsumerDoc = "Use new consumer."
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", ZkConnectDoc)
                             .withRequiredArg
                             .describedAs("urls")
                             .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server to connect to")
                                   .ofType(classOf[String])
    val groupOpt = parser.accepts("group", GroupDoc)
                         .withRequiredArg
                         .describedAs("consumer group")
                         .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val deleteOpt = parser.accepts("delete", DeleteDoc)
    val newConsumerOpt = parser.accepts("new-consumer", NewConsumerDoc)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                  .withRequiredArg
                                  .describedAs("command config property file")
                                  .ofType(classOf[String])
    val options = parser.parse(args : _*)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt, deleteOpt)

    def checkArgs() {
      // check required args
      if (options.has(newConsumerOpt)) {
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

        if (options.has(zkConnectOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $zkConnectOpt is not valid with $newConsumerOpt")

        if (options.has(deleteOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $deleteOpt is not valid with $newConsumerOpt. Note that " +
            "there's no need to delete group metadata for the new consumer as it is automatically deleted when the last " +
            "member leaves")

      } else {
        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)

        if (options.has(bootstrapServerOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $bootstrapServerOpt is only valid with $newConsumerOpt")

      }

      if (options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)
      if (options.has(deleteOpt) && !options.has(groupOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "Option %s either takes %s, %s, or both".format(deleteOpt, groupOpt, topicOpt))

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allConsumerGroupLevelOpts - describeOpt - deleteOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, allConsumerGroupLevelOpts - deleteOpt)
    }
  }
}
