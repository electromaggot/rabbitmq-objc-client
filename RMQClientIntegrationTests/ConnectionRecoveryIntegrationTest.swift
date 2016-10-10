// This source code is dual-licensed under the Mozilla Public License ("MPL"),
// version 1.1 and the Apache License ("ASL"), version 2.0.
//
// The ASL v2.0:
//
// ---------------------------------------------------------------------------
// Copyright 2016 Pivotal Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------
//
// The MPL v1.1:
//
// ---------------------------------------------------------------------------
// The contents of this file are subject to the Mozilla Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// https://www.mozilla.org/MPL/
// 
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
// License for the specific language governing rights and limitations
// under the License.
//
// The Original Code is RabbitMQ
//
// The Initial Developer of the Original Code is Pivotal Software, Inc.
// All Rights Reserved.
//
// Alternatively, the contents of this file may be used under the terms
// of the Apache Standard license (the "ASL License"), in which case the
// provisions of the ASL License are applicable instead of those
// above. If you wish to allow use of your version of this file only
// under the terms of the ASL License and not to allow others to use
// your version of this file under the MPL, indicate your decision by
// deleting the provisions above and replace them with the notice and
// other provisions required by the ASL License. If you do not delete
// the provisions above, a recipient may use your version of this file
// under either the MPL or the ASL License.
// ---------------------------------------------------------------------------

import XCTest

enum RecoveryTestError : Error {
    case timeOutWaitingForConnectionCountToDrop
}

class ConnectionRecoveryIntegrationTest: XCTestCase {
    let amqpLocalhost = "amqp://guest:guest@127.0.0.1"
    let httpAPI = RMQHTTP("http://guest:guest@127.0.0.1:15672/api")

    func testRecoversFromSocketDisconnect() {
        let recoveryInterval = 5
        let recoveryTimeout: TimeInterval = 30
        let semaphoreTimeout: Double = 30
        let confirmationTimeout = 5
        let delegate = ConnectionDelegateSpy()

        let tlsOptions = RMQTLSOptions.fromURI(amqpLocalhost)
        let transport = RMQTCPSocketTransport(host: "127.0.0.1", port: 5672, tlsOptions: tlsOptions)

        let conn = ConnectionHelper.makeConnection(recoveryInterval: recoveryInterval, transport: transport, delegate: delegate)
        conn.start()
        defer { conn.blockingClose() }

        let ch = conn.createChannel()
        let q = ch.queue("", options: [.Exclusive], arguments: ["x-max-length" : RMQShort(3)])
        let ex1 = ch.direct("foo", options: [.AutoDelete])
        let ex2 = ch.direct("bar", options: [.AutoDelete])
        let consumerSemaphore = DispatchSemaphore(value: 0)
        let confirmSemaphore = DispatchSemaphore(value: 0)

        ex2.bind(ex1)
        q.bind(ex2)

        var messages: [RMQMessage] = []
        let consumer = q.subscribe { m in
            messages.append(m)
            dispatch_semaphore_signal(consumerSemaphore)
        }

        ch.confirmSelect()

        ex1.publish("before close".dataUsingEncoding(String.Encoding.utf8))
        XCTAssertEqual(0, consumerSemaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout)),
                       "Timed out waiting for message")

        transport.simulateDisconnect()

        XCTAssert(TestHelper.pollUntil(recoveryTimeout) { delegate.recoveredConnection != nil },
                  "Didn't finish recovery")

        q.publish("after close 1".dataUsingEncoding(String.Encoding.utf8))
        consumerSemaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))
        ex1.publish("after close 2".dataUsingEncoding(String.Encoding.utf8))
        consumerSemaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))

        var acks: Set<NSNumber>?
        var nacks: Set<NSNumber>?
        ch.afterConfirmed(confirmationTimeout) { (a, n) in
            acks = a
            nacks = n
            dispatch_semaphore_signal(confirmSemaphore)
        }

        XCTAssertEqual(3, messages.count)
        XCTAssertEqual("before close".dataUsingEncoding(String.Encoding.utf8), messages[0].body)
        XCTAssertEqual("after close 1".dataUsingEncoding(String.Encoding.utf8), messages[1].body)
        XCTAssertEqual("after close 2".dataUsingEncoding(String.Encoding.utf8), messages[2].body)

        XCTAssertEqual(0, confirmSemaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout)))
        XCTAssertEqual(acks!.union(nacks!), [1, 2, 3],
                       "Didn't receive acks or nacks for publications")

        // test recovery of queue arguments - in this case, x-max-length
        consumer.cancel()
        q.publish("4".dataUsingEncoding(String.Encoding.utf8))
        q.publish("5".dataUsingEncoding(String.Encoding.utf8))
        q.publish("6".dataUsingEncoding(String.Encoding.utf8))
        q.publish("7".dataUsingEncoding(String.Encoding.utf8))

        var messagesPostCancel: [RMQMessage] = []
        q.subscribe { m in
            messagesPostCancel.append(m)
            dispatch_semaphore_signal(consumerSemaphore)
        }

        for _ in 5...7 {
            XCTAssertEqual(0, consumerSemaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout)))
        }
        XCTAssertEqual("5".dataUsingEncoding(String.Encoding.utf8), messagesPostCancel[0].body)
        XCTAssertEqual("6".dataUsingEncoding(String.Encoding.utf8), messagesPostCancel[1].body)
        XCTAssertEqual("7".dataUsingEncoding(String.Encoding.utf8), messagesPostCancel[2].body)
    }

    func testReenablesConsumersOnEachRecoveryFromConnectionClose() {
        let recoveryInterval = 2
        let semaphoreTimeout: Double = 30
        let delegate = ConnectionDelegateSpy()

        let conn = RMQConnection(uri: amqpLocalhost,
                                 tlsOptions: RMQTLSOptions.fromURI(amqpLocalhost),
                                 channelMax: RMQChannelLimit,
                                 frameMax: RMQFrameMax,
                                 heartbeat: 10,
                                 syncTimeout: 10,
                                 delegate: delegate,
                                 delegateQueue: DispatchQueue.main,
                                 recoverAfter: recoveryInterval,
                                 recoveryAttempts: 2,
                                 recoverFromConnectionClose: true)
        conn.start()
        defer { conn.blockingClose() }
        let ch = conn.createChannel()
        let q = ch.queue("", options: [.AutoDelete, .Exclusive])
        let ex = ch.direct("foo", options: [.AutoDelete])
        let semaphore = DispatchSemaphore(value: 0)
        var messages: [RMQMessage] = []

        q.bind(ex)

        q.subscribe { m in
            messages.append(m)
            dispatch_semaphore_signal(semaphore)
        }

        ex.publish("before close".dataUsingEncoding(String.Encoding.utf8))
        XCTAssertEqual(0, semaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout)),
                       "Timed out waiting for message")

        try! closeAllConnections()

        XCTAssert(TestHelper.pollUntil { delegate.recoveredConnection != nil },
                  "Didn't finish recovery the first time")
        delegate.recoveredConnection = nil

        q.publish("after close 1".dataUsingEncoding(String.Encoding.utf8))
        semaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))
        ex.publish("after close 2".dataUsingEncoding(String.Encoding.utf8))
        semaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))

        XCTAssertEqual("before close".dataUsingEncoding(String.Encoding.utf8), messages[0].body)
        XCTAssertEqual("after close 1".dataUsingEncoding(String.Encoding.utf8), messages[1].body)
        XCTAssertEqual("after close 2".dataUsingEncoding(String.Encoding.utf8), messages[2].body)

        try! closeAllConnections()

        XCTAssert(TestHelper.pollUntil { delegate.recoveredConnection != nil },
                  "Didn't finish recovery the second time")

        q.publish("after close 3".dataUsingEncoding(String.Encoding.utf8))
        semaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))
        ex.publish("after close 4".dataUsingEncoding(String.Encoding.utf8))
        semaphore.wait(timeout: TestHelper.dispatchTimeFromNow(semaphoreTimeout))

        XCTAssertEqual("before close".dataUsingEncoding(String.Encoding.utf8), messages[0].body)
        XCTAssertEqual("after close 1".dataUsingEncoding(String.Encoding.utf8), messages[1].body)
        XCTAssertEqual("after close 2".dataUsingEncoding(String.Encoding.utf8), messages[2].body)
        XCTAssertEqual("after close 3".dataUsingEncoding(String.Encoding.utf8), messages[3].body)
        XCTAssertEqual("after close 4".dataUsingEncoding(String.Encoding.utf8), messages[4].body)
    }

    fileprivate func connections() -> [RMQHTTPConnection] {
        return RMQHTTPParser().connections(httpAPI.get("/connections"))
    }

    fileprivate func closeAllConnections() throws {
        let conns = connections()
        XCTAssertGreaterThan(conns.count, 0)

        for conn in conns {
            let escapedName = conn.name.addingPercentEncoding(withAllowedCharacters: .urlHostAllowed())!
            let path = "/connections/\(escapedName)"
            httpAPI.delete(path)
        }

        if (!TestHelper.pollUntil(30) { self.connections().count == 0 }) {
            throw RecoveryTestError.timeOutWaitingForConnectionCountToDrop
        }
    }

}
