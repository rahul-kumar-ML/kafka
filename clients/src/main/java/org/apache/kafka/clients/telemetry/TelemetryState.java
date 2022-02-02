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

package org.apache.kafka.clients.telemetry;

public enum TelemetryState {

    initialized,
    subscription_needed,
    subscription_in_progress,
    push_needed,
    push_in_progress,
    terminating,
    terminating_push_in_progress,
    terminated;

    public boolean isNetworkState() {
        return
            this == subscription_in_progress ||
                this == push_in_progress ||
                this== terminating_push_in_progress;
    }

    public TelemetryState validateTransition(TelemetryState newState) {
        switch (this) {
            case initialized:
                // If we're just starting up, the happy path is to next request a subscription.
                //
                // Don't forget, there are probably error cases for which we might transition
                // straight to terminating without having done any "real" work.
                return validate(newState, subscription_needed, terminating);

            case subscription_needed:
                // If we need a subscription, the main thing we can do is request one.
                //
                // However, it's still possible that we don't get very far before terminating.
                return validate(newState, subscription_in_progress, terminating);

            case subscription_in_progress:
                // If we are finished awaiting our subscription, the most likely step is to next
                // push the telemetry. But, it's possible for there to be no telemetry requested,
                // at which point we would go back to waiting a bit before requesting the next
                // subscription.
                //
                // As before, it's possible that we don't get our response before we have to
                // terminate.
                return validate(newState, push_needed, subscription_needed, terminating);

            case push_needed:
                // If we are transitioning out of this state, chances are that we are doing so
                // because we want to push the telemetry. Alternatively, it's possible for the
                // push to fail (network issues, the subscription might have changed, etc.),
                // at which point we would again go back to waiting and requesting the next
                // subscription.
                //
                // But guess what? Yep - it's possible that we don't get to push before we have
                // to terminate.
                return validate(newState, push_in_progress, subscription_needed, terminating);

            case push_in_progress:
                // If we are transitioning out of this state, I'm guessing it's because we
                // did a successful push. We're going to want to sit tight before requesting
                // our subscription.
                //
                // But it's also possible that the push failed (again: network issues, the
                // subscription might have changed, etc.). We're not going to attempt to
                // re-push, but rather, take a breather and wait to request the
                // next subscription.
                //
                // So in either case, noting that we're now waiting for a subscription is OK.
                //
                // Again, it's possible that we don't get our response before we have to terminate.
                return validate(newState, subscription_needed, terminating);

            case terminating:
                // If we are moving out of this state, we are hopefully doing so because we're
                // going to try to send our last push. Either that or we want to be fully
                // terminated.
                return validate(newState, terminated, terminating_push_in_progress);

            case terminating_push_in_progress:
                // If we are done in this state, we should only be transitioning to fully
                // terminated.
                return validate(newState, terminated);

            case terminated:
                // We should never be able to transition out of this state...
                return validate(newState);

            default:
                // We shouldn't get to here unless we somehow accidentally try to transition out
                // of the terminated state.
                return validate(newState);
        }
    }

    private TelemetryState validate(TelemetryState newState, TelemetryState... allowableStates) {
        if (allowableStates != null) {
            for (TelemetryState allowableState : allowableStates) {
                if (newState == allowableState)
                    return newState;
            }
        }

        throw new IllegalTelemetryStateException(String.format("Invalid transition from %s to %s", this, newState));
    }

}
