/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import kafka.utils.{Time, SystemTime, ZkUtils, Logging}
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import scala.collection.JavaConversions._

/**
 * Handle the notificationMessage.
 */
trait NotificationHandler {
  def processNotification(notificationMessage: Option[String])
}

/**
 * A listener that subscribes to seqNodeRoot for any child changes where all children are assumed to be sequence node
 * with seqNodePrefix. When a child is added under seqNodeRoot this class gets notified, it looks at lastExecutedChange
 * number to avoid duplicate processing and if it finds an unprocessed child, it reads its data and calls supplied
 * notificationHandler's processNotification() method with the child's data as argument. As part of processing these changes it also
 * purges any children with currentTime - createTime > changeExpirationMs.
 *
 * The caller/user of this class should ensure that they use zkClient.subscribeStateChanges and call processAllNotifications
 * method of this class from ZkStateChangeListener's handleNewSession() method. This is necessary to ensure that if zk session
 * is terminated and reestablished any missed notification will be processed immediately.
 * @param zkClient
 * @param seqNodeRoot
 * @param seqNodePrefix
 * @param notificationHandler
 * @param changeExpirationMs
 * @param time
 */
class ZkNodeChangeNotificationListener(private val zkClient: ZkClient,
                                       private val seqNodeRoot: String,
                                       private val seqNodePrefix: String,
                                       private val notificationHandler: NotificationHandler,
                                       private val changeExpirationMs: Long = 15 * 60 * 1000,
                                       private val time: Time = SystemTime) extends Logging {
  private var lastExecutedChange = -1L

  /**
   * create seqNodeRoot and begin watching for any new children nodes.
   */
  def startup() {
    ZkUtils.makeSurePersistentPathExists(zkClient, seqNodeRoot)
    zkClient.subscribeChildChanges(seqNodeRoot, NodeChangeListener)
    processAllNotifications()
  }

  /**
   * Process all changes
   */
  def processAllNotifications() {
    val changes = zkClient.getChildren(seqNodeRoot)

    processNotifications(changes.sorted)
  }

  /**
   * Process the given list of notifications
   */
  private def processNotifications(notifications: Seq[String]) {
    if (notifications.nonEmpty) {
      info(s"Processing notification(s) to $seqNodeRoot")
      val now = time.milliseconds
      for (notification <- notifications) {
        val changeId = changeNumber(notification)
        if (changeId > lastExecutedChange) {
          val changeZnode = seqNodeRoot + "/" + notification
          val (data, stat) = ZkUtils.readDataMaybeNull(zkClient, changeZnode)
          notificationHandler.processNotification(data)
        }
        lastExecutedChange = changeId
      }
      purgeObsoleteNotifications(now, notifications)
    }
  }

  /**
   * Purges expired notifications.
   * @param now
   * @param notifications
   */
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]) {
    for (notification <- notifications.sorted) {
      val notificationNode = seqNodeRoot + "/" + notification
      val (data, stat) = ZkUtils.readDataMaybeNull(zkClient, notificationNode)
      if (data.isDefined) {
        if (now - stat.getCtime > changeExpirationMs) {
          debug(s"Purging change notification $notificationNode")
          ZkUtils.deletePath(zkClient, notificationNode)
        }
      }
    }
  }

  /* get the change number from a change notification znode */
  private def changeNumber(name: String): Long = name.substring(seqNodePrefix.length).toLong

  /**
   * A listener that gets invoked when a node is created to notify changes.
   */
  object NodeChangeListener extends IZkChildListener {
    override def handleChildChange(path: String, notifications: java.util.List[String]) {
      try {
        import scala.collection.JavaConverters._
        processNotifications(notifications.asScala.sorted)
      } catch {
        case e: Exception => error(s"Error processing notification change for path = $path and notification= $notifications :", e)
      }
    }
  }

}

