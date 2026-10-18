/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplekafka.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ControllerTest {
    @Test
    void assignmentRotatesThePreferredLeader() {
        Map<Integer, List<Integer>> assignment = Controller.assign(4, 2, List.of(0, 1, 2));
        assertEquals(List.of(0, 1), assignment.get(0));
        assertEquals(List.of(1, 2), assignment.get(1));
        assertEquals(List.of(2, 0), assignment.get(2));
        assertEquals(List.of(0, 1), assignment.get(3));
    }
}
