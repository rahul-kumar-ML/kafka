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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.stream.IntStream;

import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.Assignment;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAPMConnectProtocol.CONNECT_PROTOCOL_V3;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAPMConnectProtocol.CONNECT_PROTOCOL_V4;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.LeaderState;

/**
 * An assignor that computes a distribution of connectors and tasks according to the incremental
 * cooperative strategy for rebalancing considering the topics that APM/Snappyflow creates. This algorithm is an
 * improvement to the current one in order to divide task based on data-in-rate.
 * Here we use a round-robin policy where in we divide the log topics of all profiles among the workers and then the
 * metric topics, trace topics and finally controll topics
 * Note that this class is NOT thread-safe.
 */
public class IncrementalCooperativeAPMAssignor implements ConnectAssignor {
    private final Logger log;
    private final Time time;
    private final int maxDelay;
    protected long scheduledRebalance;
    protected int delay;
    protected int previousGenerationId;
    protected Set<String> previousMembers;

    public IncrementalCooperativeAPMAssignor(LogContext logContext, Time time, int maxDelay) {
        this.log = logContext.logger(IncrementalCooperativeAPMAssignor.class);
        this.time = time;
        this.maxDelay = maxDelay;
        this.scheduledRebalance = 0;
        this.delay = 0;
        this.previousGenerationId = -1;
        this.previousMembers = Collections.emptySet();
    }

    @Override
    public Map<String, ByteBuffer> performAssignment(String leaderId, String protocol,
                                                     List<JoinGroupResponseMember> allMemberMetadata,
                                                     WorkerCoordinator coordinator) {
        log.debug("Performing task assignment");

        Map<String, ExtendedWorkerState> memberConfigs = new HashMap<>();
        for (JoinGroupResponseMember member : allMemberMetadata) {
            memberConfigs.put(
                    member.memberId(),
                    IncrementalCooperativeAPMConnectProtocol.deserializeMetadata(ByteBuffer.wrap(member.metadata())));
        }
        log.debug("Member configs: {}", memberConfigs);

        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        long maxOffset = memberConfigs.values().stream().map(ExtendedWorkerState::offset).max(Long::compare).get();
        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                maxOffset, coordinator.configSnapshot().offset());

        short protocolVersion = memberConfigs.values().stream()
                .allMatch(state -> state.assignment().version() == CONNECT_PROTOCOL_V4)
                ? CONNECT_PROTOCOL_V4
                : CONNECT_PROTOCOL_V3;

        Long leaderOffset = ensureLeaderConfig(maxOffset, coordinator);
        if (leaderOffset == null) {
            Map<String, ExtendedAssignment> assignments = fillAssignments(
                    memberConfigs.keySet(), Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset, Collections.emptyMap(),
                    Collections.emptyMap(), Collections.emptyMap(), 0, protocolVersion);
            return serializeAssignments(assignments);
        }
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs, coordinator, protocolVersion);
    }

    private Long ensureLeaderConfig(long maxOffset, WorkerCoordinator coordinator) {
        // If this leader is behind some other members, we can't do assignment
        if (coordinator.configSnapshot().offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = coordinator.configFreshSnapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. "
                        + "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                coordinator.configSnapshot(updatedSnapshot);
                return updatedSnapshot.offset();
            }
        }
        return maxOffset;
    }

    /**
     * Performs task assignment based on the incremental cooperative connect protocol.
     * Read more on the design and implementation in:
     * {@see https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect}
     *
     * @param leaderId        the ID of the group leader
     * @param maxOffset       the latest known offset of the configuration topic
     * @param memberConfigs   the metadata of all the members of the group as gather in the current
     *                        round of rebalancing
     * @param coordinator     the worker coordinator instance that provide the configuration snapshot
     *                        and get assigned the leader state during this assignment
     * @param protocolVersion the Connect subprotocol version
     * @return the serialized assignment of tasks to the whole group, including assigned or
     * revoked tasks
     */
    protected Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset,
                                                            Map<String, ExtendedWorkerState> memberConfigs,
                                                            WorkerCoordinator coordinator, short protocolVersion) {
        log.debug("Performing task assignment during generation: {} with memberId: {}",
                coordinator.generationId(), coordinator.memberId());

        int lastCompletedGenerationId = coordinator.lastCompletedGenerationId();
        if (previousGenerationId != lastCompletedGenerationId) {
            log.debug("Clearing the slate due to generation mismatch between "
                            + "previous generation ID {} and last completed generation ID {}. "
                            + "This can happen if the leader fails to sync the assignment within a re-balancing round "
                            + "or some other worker was chosen as a leader. "
                            + "The following view of previous assignments might be outdated and will be "
                            + "ignored by the leader in the current computation of new assignments. "
                            + "Possibly outdated scheduled re-balance value: {}, "
                            + "Possibly outdated delay value: {}, "
                            + "Possibly outdated previous members: {}",
                    previousGenerationId, lastCompletedGenerationId, scheduledRebalance, delay, previousMembers);
            this.scheduledRebalance = 0;
            this.delay = 0;
            this.previousMembers = Collections.emptySet();
        }

        ClusterConfigState snapshot = coordinator.configSnapshot();
        Set<String> configuredConnectors = new TreeSet<>(snapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = configuredConnectors.stream()
                .flatMap(c -> snapshot.tasks(c).stream())
                .collect(Collectors.toSet());

        // The set of configured connectors-and-tasks
        ConnectorsAndTasks configured = new ConnectorsAndTasks.Builder().with(configuredConnectors, configuredTasks).build();
        log.debug("Configured connectors and tasks: {}", configured);

        // Current allocation of connectors-and-tasks to workers (workers that existed up till previous rebalance)
        Map<String, ConnectorsAndTasks> currentAllocation = getCurrentAllocation(memberConfigs);
        Map<String, Collection<String>> currentConnectors = currentAllocation.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().connectors()));
        Map<String, Collection<ConnectorTaskId>> currentTasks = currentAllocation.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().tasks()));
        log.debug("Current allocation is: {}", currentAllocation);

        // New allocation of connectors-and-tasks to workers. This also assumes the worker is dead or alive based on
        // set delay etc.
        Map<String, ConnectorsAndTasks> newAllocation = getNewAllocation(configuredConnectors, configuredTasks, currentAllocation);
        Map<String, Collection<String>> newConnectors = newAllocation.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().connectors()));
        Map<String, Collection<ConnectorTaskId>> newTasks = newAllocation.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, k -> k.getValue().tasks()));
        log.debug("New allocation is: {}", newAllocation);

        // connectors-and-tasks to revoke from workers
        Map<String, ConnectorsAndTasks> toRevoke = currentAllocation.keySet().stream().collect(Collectors.toMap(k -> k, k -> diff(currentAllocation.get(k), newAllocation.get(k))));
        log.debug("connectors and tasks to revoke are: {}", toRevoke);

        // connectors that need to be assigned after this rebalance round
        Map<String, Collection<String>> incrementalConnectorAllocations = diff(newConnectors, currentConnectors);
        for (Map.Entry<String, Collection<String>> entry : incrementalConnectorAllocations.entrySet()) {
            for (ConnectorsAndTasks values : toRevoke.values()) {
                entry.getValue().removeAll(values.connectors());
            }
        }

        // task that need to be assigned after this rebalance round
        Map<String, Collection<ConnectorTaskId>> incrementalTaskAllocations = diff(newTasks, currentTasks);
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : incrementalTaskAllocations.entrySet()) {
            for (ConnectorsAndTasks values : toRevoke.values()) {
                entry.getValue().removeAll(values.tasks());
            }
        }

        log.debug("Incremental connector allocation: {}", incrementalConnectorAllocations);
        log.debug("Incremental task allocation: {}", incrementalTaskAllocations);

        // initializing leader state after removing deleted tasks and connectors
        initLeaderState(coordinator, memberConfigs, configured);

        // Using incremental assignments and revocations, fill assignments in a way that follows existing schema
        Map<String, ExtendedAssignment> assignments = fillAssignments(memberConfigs.keySet(), Assignment.NO_ERROR, leaderId, memberConfigs.get(leaderId).url(), maxOffset, incrementalConnectorAllocations, incrementalTaskAllocations, toRevoke, delay, protocolVersion);
        previousGenerationId = coordinator.generationId();
        log.debug("Actual assignments that will be sent to to group coordinator: {}", assignments);
        return serializeAssignments(assignments);
    }

    private void initLeaderState(WorkerCoordinator coordinator, Map<String, ExtendedWorkerState> memberConfigs, ConnectorsAndTasks configured) {

        // Removed all deleted connectors
        Map<String, Collection<String>> connectorAllocation = memberConfigs.keySet().stream().collect(Collectors.toMap(k -> k, k -> memberConfigs.get(k).assignment().connectors()));
        for (Map.Entry<String, Collection<String>> entry : connectorAllocation.entrySet()) {
            entry.getValue().retainAll(configured.connectors());
        }

        // Removed all deleted tasks
        Map<String, Collection<ConnectorTaskId>> taskAllocation = memberConfigs.keySet().stream().collect(Collectors.toMap(k -> k, k -> memberConfigs.get(k).assignment().tasks()));
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : taskAllocation.entrySet()) {
            entry.getValue().retainAll(configured.tasks());
        }

        log.debug("Initializing leader state with Member configs: {}, Connector allocation: {}, Task allocation: {}",
                memberConfigs, connectorAllocation, taskAllocation);
        coordinator.leaderState(new LeaderState(memberConfigs, connectorAllocation, taskAllocation));
    }

    private Map<String, ConnectorsAndTasks> getCurrentAllocation(Map<String, ExtendedWorkerState> memberConfigs) {
        return memberConfigs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new ConnectorsAndTasks.Builder().with(entry.getValue().assignment().connectors(), entry.getValue().assignment().tasks()).build()));
    }

    private Map<String, ConnectorsAndTasks> getNewAllocation(Set<String> configuredConnectors, Set<ConnectorTaskId> configuredTasks, Map<String, ConnectorsAndTasks> currentAllocation) {

        List<String> workers = new ArrayList<>(currentAllocation.keySet());
        Collection<String> missingWorkers = diff(previousMembers, workers);

        if (!missingWorkers.isEmpty()) {

            log.debug("Found few missing workers: {}", missingWorkers);
            final long now = time.milliseconds();

            if (scheduledRebalance > 0 && now >= scheduledRebalance) {

                // delayed rebalance expired and it's time to assign resources using existing workers
                resetDelay();
                previousMembers = new HashSet<>(workers);
                log.debug("Set Previous Members to {}", previousMembers);

                missingWorkers = new ArrayList<>();
                log.debug("Rebalance delay has expired. Assuming missing workers as dead");

            } else {

                if (now < scheduledRebalance) {

                    // a delayed rebalance is in progress, but it's not yet time to forget the missing workers
                    delay = calculateDelay(now);
                    log.debug("Rebalance delay has begun but not yet expired. Assuming missing workers can come back. "
                            + "Updated delay value: {}", delay);

                } else {

                    // This means scheduledRebalance == 0. Hence set the scheduledRebalance using maxDelay
                    delay = maxDelay;
                    log.debug("Rebalance delay has not yet begun. Assuming missing workers can come back. "
                            + "Setting rebalance delay to the max: {}.", delay);
                }

                scheduledRebalance = now + delay;
                workers.addAll(missingWorkers);
            }

        } else {

            previousMembers = new HashSet<>(workers);
            log.debug("Set Previous Members to {}", previousMembers);
            resetDelay();
        }

        Collections.sort(workers);
        log.debug("Sorted list of workers (includes missing workers): {}", workers);

        Map<String, Set<ConnectorTaskId>> newTaskAllocation = getNewTaskAllocation(configuredConnectors, configuredTasks, currentAllocation, workers);
        Map<String, Set<String>> newConnectorAllocation = getNewConnectorAllocation(configuredConnectors, workers);

        workers.removeAll(missingWorkers);

        return workers.stream().collect(Collectors.toMap(k -> k, k -> new ConnectorsAndTasks.Builder().with(newConnectorAllocation.get(k), newTaskAllocation.get(k)).build()));
    }

    private Map<String, Set<String>> getNewConnectorAllocation(Set<String> configuredConnectors, List<String> workers) {

        Map<String, Set<String>> newAllocation = new HashMap<>();

        for (String worker : workers) {
            newAllocation.computeIfAbsent(worker, k -> new HashSet<>());
        }

        // Perform round-robin assignment of connectors
        int count = 0;
        for (String connector : configuredConnectors) {
            int index = count % workers.size();
            String worker = workers.get(index);
            newAllocation.get(worker).add(connector);
            count++;
        }

        return newAllocation;
    }

    private Map<String, Set<ConnectorTaskId>> getNewTaskAllocation(Set<String> configuredConnectors, Set<ConnectorTaskId> configuredTasks, Map<String, ConnectorsAndTasks> currentAllocation, List<String> workers) {

        // Denotes the distribution of tasks
        Map<String, Set<ConnectorTaskId>> newAllocation = new HashMap<>();

        // Denotes the distribution of task groups
        Map<String, List<TaskGroup>> intermediateAllocation = new HashMap<>();

        for (String worker : workers) {
            newAllocation.computeIfAbsent(worker, k -> new HashSet<>());
            intermediateAllocation.computeIfAbsent(worker, k -> new ArrayList<>());
        }

        List<TaskGroup> allGroups = new ArrayList<>();

        // Below loop i.e. 1 to 5 denotes the task groups. 1st group is for log data, 2nd is for metric and so on
        // es connectors will need access to all five groups but archival connectors only need first 2 groups
        // Above condition has been handled in getTaskGroup function
        for (int t : IntStream.range(1, 6).toArray()) {
            for (String connector : configuredConnectors.stream().sorted().collect(Collectors.toList())) {
                TaskGroup group = getTaskGroup(connector, configuredTasks, t);
                if (group != null) {
                    // Assign same task group object as many times as there are tasks in it.
                    // Later phases will using group.pop which will reduce the tasks in the object
                    for (int i = 0; i < group.size(); i++) {
                        allGroups.add(group);
                    }
                }
            }
        }

        // Perform round-robin assignment of task groups
        int count = 0;
        for (TaskGroup group : allGroups) {
            int index = count % workers.size();
            String worker = workers.get(index);
            intermediateAllocation.get(worker).add(group);
            count++;
        }

        // First compare task group allocation with current task allocation to determine if something can be retained
        // i.e. If worker1 is currently running C1T0, is supposed to run C1T1 in new assignment and
        // C1T0, C1T1 belong to same task group (consuming from same topic) then we can retain C1T0 itself
        // Our main goal is to have least revocations
        for (String currentWorker : currentAllocation.keySet().stream().sorted().collect(Collectors.toList())) {

            for (ConnectorTaskId taskId : currentAllocation.get(currentWorker).tasks()) {

                for (int index = 0; index < intermediateAllocation.get(currentWorker).size(); index++) {

                    TaskGroup group = intermediateAllocation.get(currentWorker).get(index);

                    if (group.contains(taskId)) {
                        group.remove(taskId);
                        intermediateAllocation.get(currentWorker).remove(index);
                        newAllocation.get(currentWorker).add(taskId);
                        break;
                    }
                }
            }
        }

        // Once the comparison with current allocation is done, assign pending tasks
        for (String worker : intermediateAllocation.keySet().stream().sorted().collect(Collectors.toList())) {
            for (TaskGroup group : intermediateAllocation.get(worker)) {
                ConnectorTaskId taskId = group.pop();
                if (taskId != null) {
                    newAllocation.get(worker).add(taskId);
                }
            }
        }

        return newAllocation;
    }

    private void resetDelay() {
        scheduledRebalance = 0;
        if (delay != 0) {
            log.debug("Resetting delay from previous value: {} to 0", delay);
        }
        delay = 0;
    }

    private static class TaskGroup {

        private final List<Integer> taskIds;
        private final String connector;

        private TaskGroup(String connector, List<Integer> taskIds) {
            this.connector = connector;
            this.taskIds = taskIds;
        }

        public boolean contains(ConnectorTaskId task) {
            return task.connector().equals(connector) && taskIds.contains(task.task());
        }

        public void remove(ConnectorTaskId task) {
            if (task.connector().equals(connector)) {
                taskIds.remove(Integer.valueOf(task.task()));
            }
        }

        public ConnectorTaskId pop() {
            if (this.taskIds.size() > 0) {
                ConnectorTaskId toReturn = new ConnectorTaskId(connector, this.taskIds.get(0));
                this.taskIds.remove(0);
                return toReturn;
            }
            return null;
        }

        public int size() {
            return this.taskIds.size();
        }
    }

    private TaskGroup getTaskGroup(String connector, Set<ConnectorTaskId> configuredTasks, Integer groupNum) {

        int numTopicsToConsumeFrom;
        List<Integer> connectorTasks = configuredTasks.stream().filter(v -> connector.equals(v.connector())).map(ConnectorTaskId::task).sorted().collect(Collectors.toList());
        int length = connectorTasks.size();

        // FixMe: rather than using 2/5 etc. check the task config to find out #topics its supposed to consume from
        if ((connector.startsWith("s3") || connector.startsWith("wasb-") ||
                connector.startsWith("gcs-")) && length % 2 == 0) {

            // Above condition checks for archival connector to have an even task count
            // as we are consuming from metric & log topics only
            numTopicsToConsumeFrom = 2;

            if (groupNum < 1 || groupNum > 2) {
                return null;
            }

        } else if (connector.startsWith("es") && length % 5 == 0) {

            // Above condition checks for ES connector to have a task count which is a multiple of 5 as we are consuming
            // from log, metric, control, trace & profile topics
            numTopicsToConsumeFrom = 5;

            if (groupNum < 1 || groupNum > 5) {
                return null;
            }

        } else {

            // Unknown connector. Hence assume that each task would consume from different topics
            numTopicsToConsumeFrom = length;

            if (groupNum != 1) {
                return null;
            }
        }

        if (numTopicsToConsumeFrom == 0) {
            return null;
        }

        // Number of task types in one group
        int groupLength = length / numTopicsToConsumeFrom;
        int itemsToSkip = groupLength * (groupNum - 1);

        List<Integer> tasksInGroup = new ArrayList<>();

        for (int i = 0; i < groupLength; i++) {
            tasksInGroup.add(connectorTasks.get(itemsToSkip + i));
        }

        return new TaskGroup(connector, tasksInGroup);
    }

    private Map<String, ExtendedAssignment> fillAssignments(Collection<String> members, short error,
                                                            String leaderId, String leaderUrl, long maxOffset,
                                                            Map<String, Collection<String>> connectorAssignments,
                                                            Map<String, Collection<ConnectorTaskId>> taskAssignments,
                                                            Map<String, ConnectorsAndTasks> revoked,
                                                            int delay, short protocolVersion) {
        Map<String, ExtendedAssignment> groupAssignment = new HashMap<>();
        for (String member : members) {
            Collection<String> connectorsToStart = connectorAssignments.getOrDefault(member, Collections.emptyList());
            Collection<ConnectorTaskId> tasksToStart = taskAssignments.getOrDefault(member, Collections.emptyList());
            Collection<String> connectorsToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).connectors();
            Collection<ConnectorTaskId> tasksToStop = revoked.getOrDefault(member, ConnectorsAndTasks.EMPTY).tasks();
            ExtendedAssignment assignment =
                    new ExtendedAssignment(protocolVersion, error, leaderId, leaderUrl, maxOffset,
                            connectorsToStart, tasksToStart, connectorsToStop, tasksToStop, delay);
            log.debug("Filling assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, assignment);
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    /**
     * From a map of workers to assignment object generate the equivalent map of workers to byte
     * buffers of serialized assignments.
     *
     * @param assignments the map of worker assignments
     * @return the serialized map of assignments to workers
     */
    protected Map<String, ByteBuffer> serializeAssignments(Map<String, ExtendedAssignment> assignments) {
        return assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> IncrementalCooperativeAPMConnectProtocol.serializeAssignment(e.getValue())));
    }

    private static ConnectorsAndTasks diff(ConnectorsAndTasks base, ConnectorsAndTasks... toSubtract) {

        Collection<String> connectors = new TreeSet<>(base.connectors());
        Collection<ConnectorTaskId> tasks = new TreeSet<>(base.tasks());

        for (ConnectorsAndTasks sub : toSubtract) {
            connectors.removeAll(sub.connectors());
            tasks.removeAll(sub.tasks());
        }

        return new ConnectorsAndTasks.Builder().with(connectors, tasks).build();
    }

    private static <T> Map<String, Collection<T>> diff(Map<String, Collection<T>> base,
                                                       Map<String, Collection<T>> toSubtract) {

        Map<String, Collection<T>> incremental = new HashMap<>();

        for (Map.Entry<String, Collection<T>> entry : base.entrySet()) {
            List<T> values = new ArrayList<>(entry.getValue());
            if (toSubtract.containsKey(entry.getKey())) {
                values.removeAll(toSubtract.get(entry.getKey()));
            }
            incremental.put(entry.getKey(), values);
        }

        return incremental;
    }

    private static Collection<String> diff(Collection<String> base, Collection<String> toSubtract) {
        Collection<String> difference = new ArrayList<>(base);
        difference.removeAll(toSubtract);
        return difference;
    }

    private int calculateDelay(long now) {
        long diff = scheduledRebalance - now;
        return diff > 0 ? (int) Math.min(diff, maxDelay) : 0;
    }
}
