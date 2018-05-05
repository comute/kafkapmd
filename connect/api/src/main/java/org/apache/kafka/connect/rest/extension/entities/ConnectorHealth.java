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
package org.apache.kafka.connect.rest.extension.entities;


import java.util.Map;
import java.util.Objects;

public class ConnectorHealth {

    private final String name;
    private final ConnectorState connector;
    private final Map<Integer, TaskState> tasks;
    private final ConnectorType type;


    public ConnectorHealth(String name,
                           ConnectorState connector,
                           Map<Integer, TaskState> tasks,
                           ConnectorType type) {
        this.name = name;
        this.connector = connector;
        this.tasks = tasks;
        this.type = type;
    }


    public String name() {
        return name;
    }


    public ConnectorState connectorState() {
        return connector;
    }


    public Map<Integer, TaskState> tasksState() {
        return tasks;
    }


    public ConnectorType type() {
        return type;
    }

    public abstract static class AbstractState {

        private final String state;
        private final String trace;
        private final String workerId;

        public AbstractState(String state, String workerId, String trace) {
            this.state = state;
            this.workerId = workerId;
            this.trace = trace;
        }

        public String state() {
            return state;
        }


        public String workerId() {
            return workerId;
        }

        public String trace() {
            return trace;
        }
    }

    public static class ConnectorState extends AbstractState {

        public ConnectorState(String state, String worker, String msg) {
            super(state, worker, msg);
        }
    }

    public static class TaskState extends AbstractState implements Comparable<TaskState> {

        private final int id;

        public TaskState(int id, String state, String worker, String msg) {
            super(state, worker, msg);
            this.id = id;
        }

        public int id() {
            return id;
        }

        @Override
        public int compareTo(TaskState that) {
            return Integer.compare(this.id, that.id);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof TaskState)) {
                return false;
            }
            TaskState other = (TaskState) o;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

}
