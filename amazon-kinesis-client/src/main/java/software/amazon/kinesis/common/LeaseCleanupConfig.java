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

package software.amazon.kinesis.common;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Configuration for lease cleanup.
 */
@Builder
@Getter
@Accessors(fluent = true)
public class LeaseCleanupConfig {
    /**
     * Interval at which to run lease cleanup thread.
     */
    private final long leaseCleanupIntervalMillis;
    /**
     * Interval at which to check if a lease is completed or not.
     */
    private final long completedLeaseCleanupIntervalMillis;
    /**
     * Interval at which to check if a lease is garbage (i.e trimmed past the stream's retention period) or not.
     */
    private final long garbageLeaseCleanupIntervalMillis;
    /**
     * Maximum number of consecutive failed attempts to delete a single lease from the lease table before the lease
     * is relinquished (see {@link #relinquishLeaseOnCleanupFailure}) instead of being retried indefinitely. This
     * guards against a lease that can never be deleted (for example, due to a restrictive IAM policy denying
     * {@code dynamodb:DeleteItem}) keeping a shard in an unrecoverable state.
     */
    @Builder.Default
    private final int maxLeaseCleanupAttempts = 10;
    /**
     * Base delay for the exponential backoff applied between failed lease cleanup attempts.
     */
    @Builder.Default
    private final long leaseCleanupBackoffBaseMillis = 5_000L;
    /**
     * Maximum delay for the exponential backoff applied between failed lease cleanup attempts.
     */
    @Builder.Default
    private final long leaseCleanupBackoffMaxMillis = 300_000L;
    /**
     * When true, a lease that cannot be deleted from the lease table due to a permanent failure (e.g.
     * {@code AccessDeniedException}) or after exhausting {@link #maxLeaseCleanupAttempts} is relinquished locally
     * (renewals are stopped) so that the lease expires and can be re-acquired by another worker, rather than
     * remaining in an unrecoverable "zombie" state where the owning consumer has stopped but the lease is never
     * released.
     */
    @Builder.Default
    private final boolean relinquishLeaseOnCleanupFailure = true;
}
