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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAPMConnectProtocol.CONNECT_PROTOCOL_V3;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAPMConnectProtocol.CONNECT_PROTOCOL_V4;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IncrementalCooperativeAPMAssignorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private WorkerCoordinator coordinator;

    @Captor
    ArgumentCaptor<Map<String, ExtendedAssignment>> assignmentsCapture;

    @Parameters
    public static Iterable<?> mode() {
        return Arrays.asList(new Object[][]{{CONNECT_PROTOCOL_V3, CONNECT_PROTOCOL_V4}});
    }

    @Parameter
    public short protocolVersion;

    private ClusterConfigState configState;
    private Map<String, ExtendedWorkerState> memberConfigs;
    private Map<String, ExtendedWorkerState> expectedMemberConfigs;
    private long offset;
    private String leader;
    private String leaderUrl;
    private Time time;
    private int rebalanceDelay;
    private IncrementalCooperativeAPMAssignor assignor;
    private int rebalanceNum;
    Map<String, ExtendedAssignment> assignments;
    Map<String, ExtendedAssignment> returnedAssignments;

    @Before
    public void setup() {
        leader = "worker1";
        leaderUrl = expectedLeaderUrl(leader);
        offset = 10;
        configState = clusterConfigState(offset, 2, 12);
        memberConfigs = memberConfigs(leader, offset, 1, 1);
        time = Time.SYSTEM;
        rebalanceDelay = 600000;
        assignments = new HashMap<>();
        initAssignor();
    }

    @After
    public void teardown() {
        verifyNoMoreInteractions(coordinator);
    }

    public void initAssignor() {
        assignor = Mockito.spy(new IncrementalCooperativeAPMAssignor(
                new LogContext(),
                time,
                rebalanceDelay));
        assignor.previousGenerationId = 1000;
    }

    /*
      In all of the below test cases we would be representing tasks as Tn-L/Tn-M/Tn-C/Tn-T
      n -> connector id
      L -> Log task
      M -> Metric task
      C -> Control task
      T -> Trace task
      We assume that an ES/S3 connector's tasks can be equally divided into 4/2 respectively.
      ES connector can have all 4 types of tasks as it consumes from all types of topics
      S3 connector can have 2 types of tasks as it consumes from Log, Metric only
      T1-L represents all tasks that fall in first quarter of connector's tasks i.e. if #tasks=8 then T1-0,T1-1
      T1-M represents all tasks that fall in second quarter of connector's tasks i.e. if #tasks=8 then T1-2,T1-3
      T1-C represents all tasks that fall in third quarter of connector's tasks i.e. if #tasks=8 then T1-4,T1-5
      T1-T represents all tasks that fall in fourth quarter of connector's tasks i.e. if #tasks=8 then T1-6,T1-7
    */

    @Test
    public void testTaskAssignmentWhenWorkerJoins() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        //
        // note: the assigned/revoked Connectors/tasks might be different, but the amount should be the same
        // assignment after this phase:
        // W1: assignedConnectors:[C0, C1], assignedTasks:[T0-L * 3, T1-L * 3, T0-M * 3, T1M * 3, T0-C * 3, T1-C * 3, T0-T * 3, T1-T * 3],
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0, C1], tasks:[T0-L * 3, T1-L * 3, T0-M * 3, T1M * 3, T0-C * 3, T1-C * 3, T0-T * 3, T1-T * 3]
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1");

        // Second assignment with a second worker joining and all connectors running on previous worker
        //
        // assignment after this phase:
        // W1: assignedConnectors:[], assignedTasks:[],
        //     revokedConnectors:[C1], revokedTasks:[T0-L * 1, T1-L * 2, T0-M * 2, T1-M * 1, T0-C * 1, T1-C * 2, T0-T * 1, T1-T * 2]
        // W2: assignedConnectors:[], assignedTasks:[]
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0], tasks:[T0-L * 2, T1-L * 1, T0-M * 1, T1-M * 2, T0-C * 2, T1-C * 1, T0-T * 2, T1-T * 1]
        // W2: connectors:[], tasks:[]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 1, 12, "worker1", "worker2");

        // Third assignment after revocations
        //
        // assignment after this phase:
        // W1: assignedConnectors:[], assignedTasks:[],
        //     revokedConnectors:[], revokedTasks:[]
        // W2: assignedConnectors:[C1], assignedTasks:[T0-L * 1, T1-L * 2, T0-M * 2, T1-M * 1, T0-C * 1, T1-C * 2, T0-T * 1, T1-T * 2]
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0], tasks:[T0-L * 2, T1-L * 1, T0-M * 1, T1-M * 2, T0-C * 2, T1-C * 1, T0-T * 2, T1-T * 1]
        // W2: connectors:[C1], tasks:[T0-L * 1, T1-L * 2, T0-M * 2, T1-M * 1, T0-C * 1, T1-C * 2, T0-T * 1, T1-T * 2]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 0, "worker1", "worker2");

        // Fourth assignment with a third worker joining and all connectors running in 2 workers
        //
        // assignment after this phase:
        // W1: assignedConnectors:[], assignedTasks:[],
        //     revokedConnectors:[], revokedTasks:[T0-L * 1, T1-M * 1, T0-C * 1, T0-T * 1]
        // W2: assignedConnectors:[], assignedTasks:[]
        //     revokedConnectors:[] revokedTasks:[T1-L * 1, T0-M * 1, T1-C * 1, T1-T * 1]
        // W3: assignedConnectors:[], assignedTasks:[]
        //     revokedConnectors:[], revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W2: connectors:[C1], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W3: connectors:[], tasks:[]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 8, "worker1", "worker2", "worker3");

        // Fifth assignment after revocations
        //
        // assignment after this phase:
        // W1: assignedConnectors:[], assignedTasks:[],
        //     revokedConnectors:[], revokedTasks:[]
        // W2: assignedConnectors:[], assignedTasks:[]
        //     revokedConnectors:[] revokedTasks:[]
        // W3: assignedConnectors:[], assignedTasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W2: connectors:[C1], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W3: connectors:[], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 8, 0, 0, "worker1", "worker2", "worker3");

        // A fourth rebalance should not change assignments
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoinAfterRevocation() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1");

        // Second assignment with a second worker joining and all connectors running on previous worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 1, 12, "worker1", "worker2");

        // Third assignment after revocations, and a third worker joining
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 4, "worker1", "worker2", "worker3");

        // Forth assignment after revocations, and a forth worker joining
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker4", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 4, 0, 4, "worker1", "worker2", "worker3", "worker4");

        // Fifth assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 4, 0, 0, "worker1", "worker2", "worker3", "worker4");

        // A sixth assignment should not change assignments
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2", "worker3", "worker4");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenWorkerLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        //
        // W1: connectors:[C0], tasks:[T0-L * 2, T1-L * 1, T0-M * 1, T1-M * 2, T0-C * 2, T1-C * 1, T0-T * 2, T1-T * 1]
        // W2: connectors:[C1], tasks:[T0-L * 1, T1-L * 2, T0-M * 2, T1-M * 1, T0-C * 1, T1-C * 2, T0-T * 1, T1-T * 2]
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group.
        // Max delay has not been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2 + 1);

        // Fourth assignment after delay expired
        //
        // assignment after this phase:
        // W1: assignedConnectors:[C1], assignedTasks:[T0-L * 1, T1-L * 2, T0-M * 2, T1-M * 1, T0-C * 1, T1-C * 2, T0-T * 1, T1-T * 2],
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0, C1], tasks:[T0-L * 3, T1-L * 3, T0-M * 3, T1-M * 3, T0-C * 3, T1-C * 3, T0-T * 3, T1-T * 3]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 0, "worker1");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenMultipleWorkersLeaveTogetherPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        //
        // note: the assigned/revoked Connectors/tasks might be different, but the amount should be the same
        // assignment after this phase: (suppose W1 is the leader)
        // W1: assignedConnectors:[C0], assignedTasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        //     revokedConnectors:[] revokedTasks:[]
        // W2: assignedConnectors:[C1], assignedTasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        //     revokedConnectors:[] revokedTasks:[]
        // W3: assignedConnectors:[C1], assignedTasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W2: connectors:[C1], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        // W3: connectors:[], tasks:[T0-L * 1, T1-L * 1, T0-M * 1, T1-M * 1, T0-C * 1, T1-C * 1, T0-T * 1, T1-T * 1]
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with only one worker remaining in the group. The workers that left the
        // group were followers. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        assignments.remove("worker3");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group.
        // Max delay has not been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2 + 1);

        // Fourth assignment after delay expired
        //
        // assignment after this phase:
        // W1: assignedConnectors:[C1], assignedTasks:[T0-L * 2, T1-L * 2, T0-M * 2, T1-M * 2, T0-C * 2, T1-C * 2, T0-T * 2, T1-T * 2],
        //     revokedConnectors:[] revokedTasks:[]
        //
        // Final distribution after this phase:
        // W1: connectors:[C0, C1], tasks:[T0-L * 3, T1-L * 3, T0-M * 3, T1-M * 3, T0-C * 3, T1-C * 3, T0-T * 3, T1-T * 3]
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 16, 0, 0, "worker1");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenMultipleWorkersLeaveSeparatelyAndPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with only two workers remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker3");

        time.sleep(rebalanceDelay / 2);

        // Third assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay continues
        applyAssignments(returnedAssignments);
        assignments.remove("worker3");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2 + 1);

        // Fourth assignment after delay expired. Only one worker remains in the group. it gets all connectors/tasks
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 16, 0, 0, "worker1");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenWorkerBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group.
        // Max delay has not been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 4);

        // Fourth assignment with the second worker returning before the delay expires
        // assignments are assigned to returning worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenMultipleWorkersBounceTogether() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with only one worker remaining in the group. The workers that left the
        // group were followers. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        assignments.remove("worker3");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with all workers returning. They get their assignments back at the end
        // Max delay has not been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 16, 0, 0, "worker1", "worker2", "worker3");

        // Fourth assignment should not cause any change
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenMultipleWorkersBounceSeparatelyButComeBackBeforeDelay() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with only two workers remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker3");

        time.sleep(rebalanceDelay / 2);

        // Third assignment with only two workers remaining in the group.
        // One worker left the group and other worker returned and
        // count down for the rebalance delay continues.
        // The returned worker gets back its earlier assignment itself
        applyAssignments(returnedAssignments);
        assignments.remove("worker3");
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 0, 0, "worker1", "worker2");

        time.sleep(rebalanceDelay / 4);

        // Fourth assignment with the second worker returning before the delay expires
        // assignments are assigned to returning worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 8, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenMultipleWorkersBounceSeparatelyButOneComesBackSoonAfterDelayExpiry() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with only two worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and
        // the count down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker3");
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        time.sleep(rebalanceDelay / 2);

        // Third assignment where in one worker returns and other worker leaves
        // Max delay has not yet been reached. Returning worker gets back assignment
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 8, 0, 0, "worker1", "worker3");

        time.sleep(rebalanceDelay / 4);

        // Fourth assignment without any change but delay is not yet expired
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(rebalanceDelay / 4, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker3");

        time.sleep(rebalanceDelay / 2 + 1);

        // Fifth assignment after delay expires. Only two workers exist in group.
        // Hence task shuffle happens to balance load
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 0, 0, "worker1", "worker3");

        // Sixth assignment with one worker returning but its too late for it to return as delay has expired.
        // It'll be considered as a new worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 1, 8, "worker1", "worker2", "worker3");

        // Seventh assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenLeaderLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking any delay
        applyAssignments(returnedAssignments);
        assignments.remove("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        memberConfigs = memberConfigs(leader, offset, assignments);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        // This treats the lost tasks as new tasks and immediately assigns them.
        // Certain connectors are revoked as well as we follow sorted round robin assignment
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 1, 0, "worker2", "worker3");

        // Final assignment which assigns the revoked connectors
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 0, 0, 0, "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenLeaderBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2", "worker3");

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking any delay
        applyAssignments(returnedAssignments);
        assignments.remove("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        memberConfigs = memberConfigs(leader, offset, assignments);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        // This treats the lost tasks as new tasks and immediately assigns them.
        // Certain connectors are revoked as well as we follow sorted round robin assignment
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 1, 0, "worker2", "worker3");

        // Fourth assignment with the previous leader returning as a follower. In this case, the
        // arrival of the previous leader is treated as an arrival of a new worker. Reassignment
        // happens immediately, first with a revocation
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker1", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 0, 1, 8, "worker1", "worker2", "worker3");

        // Fifth assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 8, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenFirstAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doThrow(new RuntimeException("Unable to send computed assignment with SyncGroupRequest"))
                .when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        try {
            expectGeneration();
            assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        } catch (RuntimeException e) {
            RequestFuture.failure(e);
        }
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment happens with members returning the same assignments (memberConfigs)
        // as the first time. Assignor returns back the same assignments as previous time
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        when(coordinator.configSnapshot()).thenReturn(configState);
        doThrow(new RuntimeException("Unable to send computed assignment with SyncGroupRequest"))
                .when(assignor).serializeAssignments(assignmentsCapture.capture());

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered.
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        try {
            expectGeneration();
            assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        } catch (RuntimeException e) {
            RequestFuture.failure(e);
        }
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 8, "worker1", "worker2", "worker3");

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time. Assignments recevied would be same as previous assignments
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertDelay(0, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 8, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFailsOutsideTheAssignor() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        expectGeneration();
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered
        // and sync group with fail on the leader worker.
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        when(coordinator.generationId())
                .thenReturn(assignor.previousGenerationId + 1)
                .thenReturn(assignor.previousGenerationId + 1);
        when(coordinator.lastCompletedGenerationId()).thenReturn(assignor.previousGenerationId - 1);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 8, "worker1", "worker2", "worker3");

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        when(coordinator.lastCompletedGenerationId()).thenReturn(assignor.previousGenerationId - 1);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertDelay(0, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 8, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAreDeleted() {
        configState = clusterConfigState(offset, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 worker and 3 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(3, 36, 0, 0, "worker1", "worker2");

        // Second assignment with an updated config state that reflects removal of a connector
        // Number of revocations might be higher than deleted connectors/tasks as round robin is based on sorted
        // collection
        configState = clusterConfigState(offset + 1, 2, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 1, 16, "worker1", "worker2");

        // Third assignment after revocations
        configState = clusterConfigState(offset + 2, 2, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 4, 0, 0, "worker1", "worker2");

        configState = clusterConfigState(offset + 3, 2, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAreAdded() {
        configState = clusterConfigState(offset, 2, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 worker and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment with an updated config state that reflects addition of a connector
        configState = clusterConfigState(offset + 1, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 4, "worker1", "worker2");

        // Third assignment which will assign back the previously revoked entities
        configState = clusterConfigState(offset + 2, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 4, 0, 0, "worker1", "worker2");

        // Fourth assignment will not make any change
        configState = clusterConfigState(offset + 3, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAndWorkersAreAdded() {
        configState = clusterConfigState(offset, 2, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 worker and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1", "worker2");

        // Second assignment with an updated config state that reflects addition of a connector and a new worker
        configState = clusterConfigState(offset + 1, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 8, "worker1", "worker2", "worker3");

        // Third assignment to assign back the revoked entities
        configState = clusterConfigState(offset + 2, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 8, 0, 0, "worker1", "worker2", "worker3");

        // Fourth assignment which will not make any change
        configState = clusterConfigState(offset + 3, 3, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        expectGeneration();
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testTaskAssignmentWhenTasksDuplicatedInWorkerAssignment() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1");

        // Second assignment with a second worker with duplicate assignment joining and all connectors running on previous worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        ExtendedAssignment duplicatedWorkerAssignment = newExpandableAssignment();
        duplicatedWorkerAssignment.connectors().addAll(newConnectors(1, 2));
        duplicatedWorkerAssignment.tasks().addAll(newTasks("es-connector1", 0, 12));
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, duplicatedWorkerAssignment));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 2, 20, "worker1", "worker2");

        // Third assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(1, 12, 0, 0, "worker1", "worker2");

        // Fourth rebalance should not change assignments
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    @Test
    public void testDuplicatedAssignmentHandleWhenTheDuplicatedAssignmentsDeleted() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(2, 24, 0, 0, "worker1");

        // delete connector1
        configState = clusterConfigState(offset, 2, 1, 12);
        when(coordinator.configSnapshot()).thenReturn(configState);

        // Second assignment with a second worker with duplicate assignment joining and the duplicated assignment is deleted at the same time
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        ExtendedAssignment duplicatedWorkerAssignment = newExpandableAssignment();
        duplicatedWorkerAssignment.connectors().addAll(newConnectors(1, 2));
        duplicatedWorkerAssignment.tasks().addAll(newTasks("es-connector1", 0, 12));
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, duplicatedWorkerAssignment));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 2, 30, "worker1", "worker2");

        // Third rebalance after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 6, 0, 0, "worker1", "worker2");

        // Fourth rebalance should not change assignments
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator, protocolVersion);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertNoReassignments(memberConfigs, expectedMemberConfigs);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

    private static List<String> newConnectors(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> "es-connector" + i)
                .collect(Collectors.toList());
    }

    private static List<ConnectorTaskId> newTasks(String connectorName, int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> new ConnectorTaskId(connectorName, i))
                .collect(Collectors.toList());
    }

    private static ClusterConfigState clusterConfigState(long offset,
                                                         int connectorNum,
                                                         int taskNum) {
        return clusterConfigState(offset, 1, connectorNum, taskNum);
    }

    private static ClusterConfigState clusterConfigState(long offset,
                                                         int connectorStart,
                                                         int connectorNum,
                                                         int taskNum) {
        int connectorNumEnd = connectorStart + connectorNum - 1;
        return new ClusterConfigState(
                offset,
                null,
                connectorTaskCounts(connectorStart, connectorNumEnd, taskNum),
                connectorConfigs(connectorStart, connectorNumEnd),
                connectorTargetStates(connectorStart, connectorNumEnd, TargetState.STARTED),
                taskConfigs(0, connectorNum, connectorNum * taskNum),
                Collections.emptySet());
    }

    private static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                  long givenOffset,
                                                                  Map<String, ExtendedAssignment> givenAssignments) {
        return givenAssignments.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, e.getValue())));
    }

    private static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                  long givenOffset,
                                                                  int start,
                                                                  int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("worker" + i, new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, null)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, Integer> connectorTaskCounts(int start,
                                                            int connectorNum,
                                                            int taskCounts) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("es-connector" + i, taskCounts))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, Map<String, String>> connectorConfigs(int start, int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("es-connector" + i, new HashMap<String, String>()))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, TargetState> connectorTargetStates(int start,
                                                                  int connectorNum,
                                                                  TargetState state) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("es-connector" + i, state))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<ConnectorTaskId, Map<String, String>> taskConfigs(int start,
                                                                         int connectorNum,
                                                                         int taskNum) {
        return IntStream.range(start, taskNum + 1)
                .mapToObj(i -> new SimpleEntry<>(
                        new ConnectorTaskId("es-connector" + i / connectorNum + 1, i),
                        new HashMap<String, String>())
                ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private void applyAssignments(Map<String, ExtendedAssignment> newAssignments) {
        newAssignments.forEach((k, v) -> {
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .connectors()
                    .removeAll(v.revokedConnectors());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .connectors()
                    .addAll(v.connectors());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .tasks()
                    .removeAll(v.revokedTasks());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .tasks()
                    .addAll(v.tasks());
        });
    }

    private ExtendedAssignment newExpandableAssignment() {
        return new ExtendedAssignment(
                protocolVersion,
                ConnectProtocol.Assignment.NO_ERROR,
                leader,
                leaderUrl,
                offset,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                0);
    }

    private static String expectedLeaderUrl(String givenLeader) {
        return "http://" + givenLeader + ":8083";
    }

    private void assertAssignment(int connectorNum, int taskNum,
                                  int revokedConnectorNum, int revokedTaskNum,
                                  String... workers) {
        assertAssignment(leader, connectorNum, taskNum, revokedConnectorNum, revokedTaskNum, workers);
    }

    private void assertAssignment(String expectedLeader, int connectorNum, int taskNum,
                                  int revokedConnectorNum, int revokedTaskNum,
                                  String... workers) {
        assertThat("Wrong number of workers",
                expectedMemberConfigs.keySet().size(),
                is(workers.length));
        assertThat("Wrong set of workers",
                new ArrayList<>(expectedMemberConfigs.keySet()), hasItems(workers));
        assertThat("Wrong number of assigned connectors",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().connectors().size()).reduce(0, Integer::sum),
                is(connectorNum));
        assertThat("Wrong number of assigned tasks",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().tasks().size()).reduce(0, Integer::sum),
                is(taskNum));
        assertThat("Wrong number of revoked connectors",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().revokedConnectors().size()).reduce(0, Integer::sum),
                is(revokedConnectorNum));
        assertThat("Wrong number of revoked tasks",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().revokedTasks().size()).reduce(0, Integer::sum),
                is(revokedTaskNum));
        assertThat("Wrong leader in assignments",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().leader()).distinct().collect(Collectors.joining(", ")),
                is(expectedLeader));
        assertThat("Wrong leaderUrl in assignments",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().leaderUrl()).distinct().collect(Collectors.joining(", ")),
                is(expectedLeaderUrl(expectedLeader)));
    }

    private void assertDelay(int expectedDelay, Map<String, ExtendedAssignment> newAssignments) {
        newAssignments.values().stream()
                .forEach(a -> assertEquals(
                        "Wrong rebalance delay in " + a, expectedDelay, a.delay()));
    }

    private void assertNoReassignments(Map<String, ExtendedWorkerState> existingAssignments,
                                       Map<String, ExtendedWorkerState> newAssignments) {
        assertNoDuplicateInAssignment(existingAssignments);
        assertNoDuplicateInAssignment(newAssignments);

        List<String> existingConnectors = existingAssignments.values().stream()
                .flatMap(a -> a.assignment().connectors().stream())
                .collect(Collectors.toList());
        List<String> newConnectors = newAssignments.values().stream()
                .flatMap(a -> a.assignment().connectors().stream())
                .collect(Collectors.toList());

        List<ConnectorTaskId> existingTasks = existingAssignments.values().stream()
                .flatMap(a -> a.assignment().tasks().stream())
                .collect(Collectors.toList());

        List<ConnectorTaskId> newTasks = newAssignments.values().stream()
                .flatMap(a -> a.assignment().tasks().stream())
                .collect(Collectors.toList());

        existingConnectors.retainAll(newConnectors);
        assertThat("Found connectors in new assignment that already exist in current assignment",
                Collections.emptyList(),
                is(existingConnectors));
        existingTasks.retainAll(newTasks);
        assertThat("Found tasks in new assignment that already exist in current assignment",
                Collections.emptyList(),
                is(existingConnectors));
    }

    private void assertNoDuplicateInAssignment(Map<String, ExtendedWorkerState> existingAssignment) {
        List<String> existingConnectors = existingAssignment.values().stream()
                .flatMap(a -> a.assignment().connectors().stream())
                .collect(Collectors.toList());
        Set<String> existingUniqueConnectors = new HashSet<>(existingConnectors);
        existingConnectors.removeAll(existingUniqueConnectors);
        assertThat("Connectors should be unique in assignments but duplicates where found",
                Collections.emptyList(),
                is(existingConnectors));

        List<ConnectorTaskId> existingTasks = existingAssignment.values().stream()
                .flatMap(a -> a.assignment().tasks().stream())
                .collect(Collectors.toList());
        Set<ConnectorTaskId> existingUniqueTasks = new HashSet<>(existingTasks);
        existingTasks.removeAll(existingUniqueTasks);
        assertThat("Tasks should be unique in assignments but duplicates where found",
                Collections.emptyList(),
                is(existingTasks));
    }

    private void expectGeneration() {
        when(coordinator.generationId())
                .thenReturn(assignor.previousGenerationId + 1)
                .thenReturn(assignor.previousGenerationId + 1);
        when(coordinator.lastCompletedGenerationId()).thenReturn(assignor.previousGenerationId);
    }
}
