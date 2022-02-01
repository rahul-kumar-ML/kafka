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

package org.apache.kafka.clients;

public enum TelemetryState {

    initialized, subscription_needed, subscription_in_progress, push_needed, push_in_progress, terminating, terminated;

    public TelemetryState validateTransition(TelemetryState newState) {
        switch (this) {
            case initialized:
                // If we're just starting up, the happy path is to next request a subscription...
                if (newState == subscription_needed)
                    return newState;

                // ...however, there are probably error cases for which we might transition
                // straight to terminating.
                if (newState == terminating)
                    return newState;

                throw fail(newState);

            case subscription_needed:
                // If we need a subscription, the main thing we can do is request one...
                if (newState == subscription_in_progress)
                    return newState;

                // ...however, it's still possible that we don't get very far before we
                // transition to terminating.
                if (newState == terminating)
                    return newState;

                throw fail(newState);

            case subscription_in_progress:
                // If we are finished awaiting our subscription, the happy path is to next
                // push the telemetry...
                if (newState == push_needed)
                    return newState;

                // ...but, it's possible for there to be no telemetry requested, at which point
                // we would go back to waiting a bit before requesting the next subscription...
                if (newState == subscription_needed)
                    return newState;

                // ...however, it's possible that we don't get our response before we have
                // to terminate.
                if (newState == terminating)
                    return newState;

                throw fail(newState);

            case push_needed:
                // If we are transitioning out of this state, chances are that we are doing so
                // because we want to push the telemetry...
                if (newState == push_in_progress)
                    return newState;

                // ...but, it's possible for the push to fail (network issues, the subscription
                // might have changed, etc.), at which point we would again go back to waiting
                // and requesting the next subscription...
                if (newState == subscription_needed)
                    return newState;

                // ...however, it's possible that we don't get our response before we have
                // to terminate.
                if (newState == terminating)
                    return newState;

                throw fail(newState);

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
                // So in either case, noting that we're now waiting for a subscription is OK...
                if (newState == subscription_needed)
                    return newState;

                // ...however, it's possible that we don't get our response before we have
                // to terminate.
                if (newState == terminating)
                    return newState;

                throw fail(newState);

            case terminating:
                // If we are done in this state, one of the two likely scenarios is that we want
                // to be fully terminated...
                if (newState == terminated)
                    return newState;

                // The second likely scenario is that we need to fire off our terminal
                // telemetry push...
                if (newState == push_needed)
                    return newState;

                throw fail(newState);

            default:
                // We shouldn't get to here unless we somehow accidentally try to transition out
                // of the terminated state.
                throw fail(newState);
        }
    }

    private IllegalStateException fail(TelemetryState newState) {
        return new IllegalStateException(String.format("Invalid transition from %s to %s", this, newState));
    }

}
