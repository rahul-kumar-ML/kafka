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

package org.apache.kafka.clients.consumer.state_machines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackgroundStateMachine {
    private static final Logger log = LoggerFactory.getLogger(BackgroundStateMachine.class);
    private BackgroundStates currentState;

    public BackgroundStateMachine(BackgroundStates initialState) {
        this.currentState = initialState;
    }

    public boolean transitionTo(BackgroundStates nextState) {
        if(validateStateTransition(currentState, nextState)) {
            this.currentState = nextState;
        }
        return false;
    }

    @Override
    public String toString() {
        return this.currentState.toString();
    }

    public boolean isStable() {
        return this.currentState == BackgroundStates.STABLE;
    }

    private boolean validateStateTransition(BackgroundStates currentState, BackgroundStates nextState) {
        switch(currentState) {
            case DOWN:
                if(nextState != BackgroundStates.INITIALIZED) {
                    log.error("INITIALIZED is not a possible next state");
                    return false;
                }
                break;
            case INITIALIZED:
                if(nextState == BackgroundStates.STABLE) {
                    return false;
                }
                break;
            case COORDINATOR_DISCOVERY:
            case STABLE:
                break;
            default:
                return false;
        }
        return true;
    }

    public BackgroundStates getCurrentState() {
        return currentState;
    }
}
