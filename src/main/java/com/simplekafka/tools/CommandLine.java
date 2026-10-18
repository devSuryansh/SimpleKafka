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

package com.simplekafka.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small {@code --key value} parser for the SimpleKafka command.
 */
final class CommandLine {
    private final String command;
    private final String action;
    private final Map<String, String> options;

    private CommandLine(String command, String action, Map<String, String> options) {
        this.command = command;
        this.action = action;
        this.options = options;
    }

    static CommandLine parse(String[] args) {
        if (args.length == 0) {
            return new CommandLine("", "", Map.of());
        }
        String command = args[0];
        int index = 1;
        String action = "";
        if (index < args.length && !args[index].startsWith("--")) {
            action = args[index];
            index++;
        }
        Map<String, String> options = new LinkedHashMap<>();
        List<String> skipped = new ArrayList<>();
        while (index < args.length) {
            String arg = args[index++];
            if (!arg.startsWith("--") || arg.length() < 3) {
                skipped.add(arg);
                continue;
            }
            String key = arg.substring(2);
            if (index < args.length && !args[index].startsWith("--")) {
                options.put(key, args[index++]);
            } else {
                options.put(key, "true");
            }
        }
        if (!skipped.isEmpty()) {
            throw new IllegalArgumentException("unexpected arguments: " + String.join(" ", skipped));
        }
        return new CommandLine(command, action, options);
    }

    String command() {
        return command;
    }

    String action() {
        return action;
    }

    boolean has(String key) {
        return options.containsKey(key);
    }

    String option(String key, String fallback) {
        return options.getOrDefault(key, fallback);
    }

    String required(String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing --" + key);
        }
        return value;
    }

    int optionInt(String key, int fallback) {
        String value = options.get(key);
        if (value == null) {
            return fallback;
        }
        return Integer.parseInt(value);
    }
}
