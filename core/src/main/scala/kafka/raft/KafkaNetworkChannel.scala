/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.raft

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, KafkaClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.{NetworkChannel, RaftRequest, RaftResponse, RaftUtil}

import scala.collection.mutable

object KafkaNetworkChannel {

  val nonRoutableAddress = new InetSocketAddress("0.0.0.0", 0)
  private[raft] def buildRequest(requestData: ApiMessage): AbstractRequest.Builder[_ <: AbstractRequest] = {
    requestData match {
      case voteRequest: VoteRequestData =>
        new VoteRequest.Builder(voteRequest)
      case beginEpochRequest: BeginQuorumEpochRequestData =>
        new BeginQuorumEpochRequest.Builder(beginEpochRequest)
      case endEpochRequest: EndQuorumEpochRequestData =>
        new EndQuorumEpochRequest.Builder(endEpochRequest)
      case fetchRequest: FetchRequestData =>
        // Since we already have the request, we go through a simplified builder
        new AbstractRequest.Builder[FetchRequest](ApiKeys.FETCH) {
          override def build(version: Short): FetchRequest = new FetchRequest(fetchRequest, version)
          override def toString(): String = fetchRequest.toString
        }
      case fetchSnapshotRequest: FetchSnapshotRequestData =>
        new FetchSnapshotRequest.Builder(fetchSnapshotRequest)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected type for requestData: $requestData")
    }
  }

}

private[raft] class RaftSendThread(
  name: String,
  networkClient: KafkaClient,
  requestTimeoutMs: Int,
  time: Time,
  isInterruptible: Boolean = true
) extends InterBrokerSendThread(
  name,
  networkClient,
  requestTimeoutMs,
  time,
  isInterruptible
) {
  private val queue = new ConcurrentLinkedQueue[RequestAndCompletionHandler]()

  def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    val buffer =  mutable.Buffer[RequestAndCompletionHandler]()
    while (true) {
      val request = queue.poll()
      if (request == null) {
        return buffer
      } else {
        buffer += request
      }
    }
    buffer
  }

  def sendRequest(request: RequestAndCompletionHandler): Unit = {
    queue.add(request)
    wakeup()
  }

}


class KafkaNetworkChannel(
  time: Time,
  client: KafkaClient,
  requestTimeoutMs: Int
) extends NetworkChannel with Logging {
  import KafkaNetworkChannel._

  type ResponseHandler = AbstractResponse => Unit

  private val correlationIdCounter = new AtomicInteger(0)
  private val endpoints = mutable.HashMap.empty[Int, Node]

  private val requestThread = new RaftSendThread(
    name = "raft-outbound-request-thread",
    networkClient = client,
    requestTimeoutMs = requestTimeoutMs,
    time = time,
    isInterruptible = false
  )

  override def send(request: RaftRequest.Outbound): Unit = {
    def completeFuture(message: ApiMessage): Unit = {
      val response = new RaftResponse.Inbound(
        request.correlationId,
        message,
        request.destinationId
      )
      request.completion.complete(response)
    }

    def onComplete(clientResponse: ClientResponse): Unit = {
      val response = if (clientResponse.authenticationException != null) {
        errorResponse(request.data, Errors.CLUSTER_AUTHORIZATION_FAILED)
      } else if (clientResponse.wasDisconnected()) {
        errorResponse(request.data, Errors.BROKER_NOT_AVAILABLE)
      } else {
        clientResponse.responseBody.data
      }
      completeFuture(response)
    }

    endpoints.get(request.destinationId) match {
      case Some(node) =>
        requestThread.sendRequest(RequestAndCompletionHandler(
          request.createdTimeMs,
          destination = node,
          request = buildRequest(request.data),
          handler = onComplete
        ))

      case None =>
        completeFuture(errorResponse(request.data, Errors.BROKER_NOT_AVAILABLE))
    }
  }

  // Visible for testing
  private[raft] def pollOnce(): Unit = {
    requestThread.doWork()
  }

  override def newCorrelationId(): Int = {
    correlationIdCounter.getAndIncrement()
  }

  private def errorResponse(
    request: ApiMessage,
    error: Errors
  ): ApiMessage = {
    val apiKey = ApiKeys.forId(request.apiKey)
    RaftUtil.errorResponse(apiKey, error)
  }

  def updateEndpoint(id: Int, address: InetSocketAddress): Unit = {
    // We update the endpoint only if it's routable/valid
    if (!address.equals(nonRoutableAddress)) {
      val node = new Node(id, address.getHostString, address.getPort)
      endpoints.put(id, node)
    }
  }

  def start(): Unit = {
    requestThread.start()
  }

  def initiateShutdown(): Unit = {
    requestThread.initiateShutdown()
  }

  override def close(): Unit = {
    requestThread.shutdown()
    client.close()
  }

}
