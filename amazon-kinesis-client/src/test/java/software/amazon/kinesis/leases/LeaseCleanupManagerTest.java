/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.LeasePendingDeletion;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LeaseCleanupManagerTest {

    private static final ShardInfo SHARD_INFO =
            new ShardInfo("shardId", "concurrencyToken", Collections.emptySet(), ExtendedSequenceNumber.LATEST);

    private static final StreamIdentifier STREAM_IDENTIFIER = StreamIdentifier.singleStreamInstance("streamName");

    private final long leaseCleanupIntervalMillis = Duration.ofSeconds(1).toMillis();
    private final long completedLeaseCleanupIntervalMillis =
            Duration.ofSeconds(0).toMillis();
    private final long garbageLeaseCleanupIntervalMillis = Duration.ofSeconds(0).toMillis();
    private final int maxLeaseCleanupAttempts = 3;
    private final long leaseCleanupBackoffBaseMillis = 0L;
    private final long leaseCleanupBackoffMaxMillis = 0L;
    private boolean relinquishLeaseOnCleanupFailure = true;
    private boolean cleanupLeasesOfCompletedShards = true;
    private LeaseCleanupManager leaseCleanupManager;
    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private ScheduledExecutorService deletionThreadPool;

    @Before
    public void setUp() throws Exception {
        leaseCleanupManager = buildLeaseCleanupManager();

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
    }

    private LeaseCleanupManager buildLeaseCleanupManager() {
        return new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis,
                maxLeaseCleanupAttempts,
                leaseCleanupBackoffBaseMillis,
                leaseCleanupBackoffMaxMillis,
                relinquishLeaseOnCleanupFailure);
    }

    /**
     * Tests subsequent calls to start {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentStarts() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.start();
    }

    /**
     * Tests subsequent calls to shutdown {@link LeaseCleanupManager}.
     */
    @Test
    public final void testSubsequentShutdowns() {
        leaseCleanupManager.start();
        Assert.assertTrue(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
        Assert.assertFalse(leaseCleanupManager.isRunning());
        leaseCleanupManager.shutdown();
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedSplitCase() throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that when both child shard leases are present, we are able to delete the parent shard for the completed
     * shard case.
     */
    @Test
    public final void testParentShardLeaseDeletedMergeCase() throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 1);
    }

    /**
     * Tests that if cleanupLeasesOfCompletedShards is not enabled by the customer, then no leases are cleaned up for
     * the completed shard case.
     */
    @Test
    public final void testNoLeasesDeletedWhenNotEnabled() throws Exception {
        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = buildLeaseCleanupManager();

        verifyExpectedDeletedLeasesCompletedShardCase(
                SHARD_INFO, childShardsForSplit(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Tests that if some of the child shard leases are missing, we fail fast and don't delete the parent shard lease
     * for the completed shard case.
     */
    @Test
    public final void testNoCleanupWhenSomeChildShardLeasesAreNotPresent() throws Exception {
        List<ChildShard> childShards = childShardsForSplit();

        verifyExpectedDeletedLeasesCompletedShardCase(SHARD_INFO, childShards, ExtendedSequenceNumber.LATEST, false, 0);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint TRIM_HORIZON),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTrim() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.TRIM_HORIZON);
    }

    /**
     * Tests that if some child shard leases haven't begun processing (at least one lease w/ checkpoint AT_TIMESTAMP),
     * we don't delete them for the completed shard case.
     */
    @Test
    public final void testParentShardLeaseNotDeletedWhenChildIsAtTimestamp() throws Exception {
        testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber.AT_TIMESTAMP);
    }

    private void testParentShardLeaseNotDeletedWhenChildIsAtPosition(ExtendedSequenceNumber extendedSequenceNumber)
            throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(SHARD_INFO, childShardsForMerge(), extendedSequenceNumber, 0);
    }

    /**
     * Tests that if a lease's parents are still present, we do not delete the lease.
     */
    @Test
    public final void testLeaseNotDeletedWhenParentsStillPresent() throws Exception {
        final ShardInfo shardInfo = new ShardInfo(
                "shardId-0", "concurrencyToken", Collections.singleton("parent"), ExtendedSequenceNumber.LATEST);

        verifyExpectedDeletedLeasesCompletedShardCase(
                shardInfo, childShardsForMerge(), ExtendedSequenceNumber.LATEST, 0);
    }

    /**
     * Verify {@link NullPointerException} is not thrown when a null lease is enqueued.
     */
    @Test
    public void testEnqueueNullLease() {
        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(null, SHARD_INFO));
    }

    /**
     * Tests ResourceNotFound case for if a shard expires, that we delete the lease when shardExpired is found.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExist() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    /**
     * Tests ResourceNotFound case when completed lease cleanup is disabled.
     */
    @Test
    public final void testLeaseDeletedWhenShardDoesNotExistAndCleanupCompletedLeaseDisabled() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        cleanupLeasesOfCompletedShards = false;

        leaseCleanupManager = buildLeaseCleanupManager();

        testLeaseDeletedWhenShardDoesNotExist(heldLease);
    }

    private void testLeaseDeletedWhenShardDoesNotExist(Lease heldLease) throws Exception {
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector).getChildShards(SHARD_INFO.shardId());
        verify(leaseRefresher).deleteLease(heldLease);
    }

    /**
     * Reproduces the COE failure mode: lease deletion is permanently denied (IAM deny on
     * {@code dynamodb:DeleteItem}). The lease must not be retried forever; instead it is relinquished on the first
     * permanent failure so another worker can re-acquire the shard rather than it becoming a zombie.
     */
    @Test
    public final void testLeaseRelinquishedOnAccessDeniedDuringCleanup() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);
        doThrow(new DependencyException(accessDeniedException()))
                .when(leaseRefresher)
                .deleteLease(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));
        leaseCleanupManager.cleanupLeases();

        // Permanent failure -> relinquish immediately (do not wait for the retry budget to be exhausted).
        verify(leaseCoordinator, times(1)).dropLease(heldLease);

        // Lease was removed from the queue, so a subsequent scan attempts no further deletions.
        leaseCleanupManager.cleanupLeases();
        verify(leaseRefresher, times(1)).deleteLease(heldLease);
    }

    /**
     * A transient (non-permanent) failure should be retried with backoff and only relinquished once the configured
     * maximum number of attempts has been reached.
     */
    @Test
    public final void testLeaseRetriedThenRelinquishedAfterMaxAttempts() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);
        doThrow(new DependencyException(new RuntimeException("transient DynamoDB failure")))
                .when(leaseRefresher)
                .deleteLease(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));

        // Backoff base is 0 in tests, so each scan retries. First (maxLeaseCleanupAttempts - 1) scans retry only.
        for (int i = 0; i < maxLeaseCleanupAttempts - 1; i++) {
            leaseCleanupManager.cleanupLeases();
        }
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));

        // The attempt that reaches maxLeaseCleanupAttempts relinquishes the lease.
        leaseCleanupManager.cleanupLeases();
        verify(leaseCoordinator, times(1)).dropLease(heldLease);
        verify(leaseRefresher, times(maxLeaseCleanupAttempts)).deleteLease(heldLease);
    }

    /**
     * When relinquishment is disabled, an undeletable lease is retried indefinitely and never relinquished
     * (preserving the legacy behavior for operators who opt out).
     */
    @Test
    public final void testLeaseNotRelinquishedWhenDisabled() throws Exception {
        relinquishLeaseOnCleanupFailure = false;
        leaseCleanupManager = buildLeaseCleanupManager();

        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);
        doThrow(new DependencyException(accessDeniedException()))
                .when(leaseRefresher)
                .deleteLease(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));
        leaseCleanupManager.cleanupLeases();
        leaseCleanupManager.cleanupLeases();

        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
        // Still re-enqueued and retried on each scan.
        verify(leaseRefresher, times(2)).deleteLease(heldLease);
    }

    /**
     * Unit-tests the access-denied classification, including unwrapping of the {@link DependencyException} that the
     * lease refresher wraps DynamoDB exceptions in.
     */
    @Test
    public final void testIsAccessDeniedFailureClassification() {
        Assert.assertTrue(LeaseCleanupManager.isAccessDeniedFailure(accessDeniedException()));
        Assert.assertTrue(LeaseCleanupManager.isAccessDeniedFailure(new DependencyException(accessDeniedException())));
        // A 403 with no modeled error code is still treated as access-denied.
        Assert.assertTrue(LeaseCleanupManager.isAccessDeniedFailure(statusOnly403Exception()));
        Assert.assertFalse(
                LeaseCleanupManager.isAccessDeniedFailure(new DependencyException(new RuntimeException("transient"))));
        Assert.assertFalse(LeaseCleanupManager.isAccessDeniedFailure(null));
    }

    /**
     * If relinquishment itself fails (e.g. {@code dropLease} throws), the lease is re-enqueued and relinquishment is
     * retried on the next scan rather than being silently dropped from the queue.
     */
    @Test
    public final void testLeaseReenqueuedWhenRelinquishFails() throws Exception {
        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);
        doThrow(new DependencyException(accessDeniedException()))
                .when(leaseRefresher)
                .deleteLease(heldLease);
        doThrow(new RuntimeException("drop failed")).when(leaseCoordinator).dropLease(heldLease);

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));

        leaseCleanupManager.cleanupLeases();
        verify(leaseCoordinator, times(1)).dropLease(heldLease);

        // Relinquish failed, so the lease remains enqueued and is retried on the next scan.
        leaseCleanupManager.cleanupLeases();
        verify(leaseCoordinator, times(2)).dropLease(heldLease);
        verify(leaseRefresher, times(2)).deleteLease(heldLease);
    }

    /**
     * Verifies the exponential backoff computation, including the cap and the overflow guard for extreme
     * configuration values.
     */
    @Test
    public final void testComputeBackoffMillisCapAndOverflow() {
        final LeaseCleanupManager manager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis,
                maxLeaseCleanupAttempts,
                5_000L,
                300_000L,
                true);

        Assert.assertEquals(5_000L, manager.computeBackoffMillis(1));
        Assert.assertEquals(10_000L, manager.computeBackoffMillis(2));
        // Large attempt count is capped at the configured maximum.
        Assert.assertEquals(300_000L, manager.computeBackoffMillis(100));

        // A base large enough to overflow when shifted must fall back to the configured maximum, never negative.
        final LeaseCleanupManager overflowManager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis,
                maxLeaseCleanupAttempts,
                Long.MAX_VALUE,
                300_000L,
                true);
        Assert.assertEquals(300_000L, overflowManager.computeBackoffMillis(2));
    }

    /**
     * Verifies that after a failed deletion a lease is not retried again until its backoff window has elapsed.
     */
    @Test
    public final void testBackoffWindowSkipsRetryUntilElapsed() throws Exception {
        // Non-zero backoff and relinquishment disabled so the lease stays enqueued and the backoff gate is exercised.
        final LeaseCleanupManager manager = new LeaseCleanupManager(
                leaseCoordinator,
                NULL_METRICS_FACTORY,
                deletionThreadPool,
                cleanupLeasesOfCompletedShards,
                leaseCleanupIntervalMillis,
                completedLeaseCleanupIntervalMillis,
                garbageLeaseCleanupIntervalMillis,
                maxLeaseCleanupAttempts,
                60_000L,
                60_000L,
                false);

        final Lease heldLease =
                LeaseHelper.createLease(SHARD_INFO.shardId(), "leaseOwner", Collections.singleton("parentShardId"));

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(shardDetector.getChildShards(any(String.class))).thenThrow(ResourceNotFoundException.class);
        when(leaseRefresher.getLease(heldLease.leaseKey())).thenReturn(heldLease);
        doThrow(new DependencyException(new RuntimeException("transient DynamoDB failure")))
                .when(leaseRefresher)
                .deleteLease(heldLease);

        manager.enqueueForDeletion(createLeasePendingDeletion(heldLease, SHARD_INFO));

        // First scan attempts deletion and fails, arming a 60s backoff window.
        manager.cleanupLeases();
        // Second scan happens immediately; the lease is still within its backoff window and must be skipped.
        manager.cleanupLeases();

        verify(leaseRefresher, times(1)).deleteLease(heldLease);
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
    }

    private static DynamoDbException accessDeniedException() {
        return (DynamoDbException) DynamoDbException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDeniedException")
                        .build())
                .statusCode(403)
                .message("User is not authorized to perform: dynamodb:DeleteItem")
                .build();
    }

    private static DynamoDbException statusOnly403Exception() {
        return (DynamoDbException)
                DynamoDbException.builder().statusCode(403).message("Forbidden").build();
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(
            ShardInfo shardInfo,
            List<ChildShard> childShards,
            ExtendedSequenceNumber extendedSequenceNumber,
            int expectedDeletedLeases)
            throws Exception {
        verifyExpectedDeletedLeasesCompletedShardCase(
                shardInfo, childShards, extendedSequenceNumber, true, expectedDeletedLeases);
    }

    private void verifyExpectedDeletedLeasesCompletedShardCase(
            ShardInfo shardInfo,
            List<ChildShard> childShards,
            ExtendedSequenceNumber extendedSequenceNumber,
            boolean childShardLeasesPresent,
            int expectedDeletedLeases)
            throws Exception {
        final Lease lease = LeaseHelper.createLease(
                shardInfo.shardId(),
                "leaseOwner",
                shardInfo.parentShardIds(),
                childShards.stream().map(ChildShard::shardId).collect(Collectors.toSet()));
        final List<Lease> childShardLeases = childShards.stream()
                .map(c -> LeaseHelper.createLease(
                        ShardInfo.getLeaseKey(shardInfo, c.shardId()),
                        "leaseOwner",
                        Collections.singleton(shardInfo.shardId()),
                        Collections.emptyList(),
                        extendedSequenceNumber))
                .collect(Collectors.toList());

        final List<Lease> parentShardLeases = lease.parentShardIds().stream()
                .map(p -> LeaseHelper.createLease(
                        ShardInfo.getLeaseKey(shardInfo, p),
                        "leaseOwner",
                        Collections.emptyList(),
                        Collections.singleton(shardInfo.shardId()),
                        extendedSequenceNumber))
                .collect(Collectors.toList());

        when(leaseRefresher.getLease(lease.leaseKey())).thenReturn(lease);
        for (Lease parentShardLease : parentShardLeases) {
            when(leaseRefresher.getLease(parentShardLease.leaseKey())).thenReturn(parentShardLease);
        }
        if (childShardLeasesPresent) {
            for (Lease childShardLease : childShardLeases) {
                when(leaseRefresher.getLease(childShardLease.leaseKey())).thenReturn(childShardLease);
            }
        }

        leaseCleanupManager.enqueueForDeletion(createLeasePendingDeletion(lease, shardInfo));
        leaseCleanupManager.cleanupLeases();

        verify(shardDetector).getChildShards(shardInfo.shardId());
        verify(leaseRefresher, times(expectedDeletedLeases)).deleteLease(any(Lease.class));
    }

    private List<ChildShard> childShardsForSplit() {
        final List<String> parentShards = Collections.singletonList("splitParent");

        ChildShard leftChild = ChildShard.builder()
                .shardId("leftChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("rightChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();

        return Arrays.asList(leftChild, rightChild);
    }

    private List<ChildShard> childShardsForMerge() {
        final List<String> parentShards = Arrays.asList("mergeParent1", "mergeParent2");

        ChildShard child = ChildShard.builder()
                .shardId("onlyChild")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "99"))
                .build();

        return Collections.singletonList(child);
    }

    private LeasePendingDeletion createLeasePendingDeletion(final Lease lease, final ShardInfo shardInfo) {
        return new LeasePendingDeletion(STREAM_IDENTIFIER, lease, shardInfo, shardDetector);
    }
}
