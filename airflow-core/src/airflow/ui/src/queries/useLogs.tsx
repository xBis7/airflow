/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { chakra, Box } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import innerText from "react-innertext";

import type { TaskInstanceResponse, TaskInstancesLogResponse } from "openapi/requests/types.gen";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { getTaskInstanceLink } from "src/utils/links";

type Props = {
  accept?: "*/*" | "application/json" | "application/x-ndjson";
  dagId: string;
  expanded?: boolean;
  limit?: number;
  logLevelFilters?: Array<string>;
  showSource?: boolean;
  showTimestamp?: boolean;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  tryNumber?: number;
};

type ParseLogsProps = {
  data: TaskInstancesLogResponse["content"];
  expanded?: boolean;
  logLevelFilters?: Array<string>;
  showSource?: boolean;
  showTimestamp?: boolean;
  sourceFilters?: Array<string>;
  taskInstance?: TaskInstanceResponse;
  translate: TFunction;
  tryNumber: number;
};

const parseLogs = ({
  data,
  expanded,
  logLevelFilters,
  showSource,
  showTimestamp,
  sourceFilters,
  taskInstance,
  translate,
  tryNumber,
}: ParseLogsProps) => {
  let warning;
  let parsedLines;
  const sources: Array<string> = [];

  const open = expanded ?? Boolean(globalThis.location.hash);
  const logLink = taskInstance ? `${getTaskInstanceLink(taskInstance)}?try_number=${tryNumber}` : "";

  try {
    parsedLines = data
      .map((datum, index) => {
        if (typeof datum !== "string" && "logger" in datum) {
          const source = datum.logger as string;

          if (!sources.includes(source)) {
            sources.push(source);
          }
        }

        return renderStructuredLog({
          index,
          logLevelFilters,
          logLink,
          logMessage: datum,
          renderingMode: "jsx",
          showSource,
          showTimestamp,
          sourceFilters,
          translate,
        });
      })
      .filter((parsedLine) => parsedLine !== "");
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "An error occurred.";

    // eslint-disable-next-line no-console
    console.warn(`Error parsing logs: ${errorMessage}`);
    warning = "Unable to show logs. There was an error parsing logs.";

    return { data, warning };
  }

  parsedLines = (() => {
    type Group = { level: number; lines: Array<JSX.Element | "">; name: string };
    const groupStack: Array<Group> = [];
    const result: Array<JSX.Element | ""> = [];

    parsedLines.forEach((line) => {
      const text = innerText(line);

      if (text.includes("::group::")) {
        const groupName = text.split("::group::")[1] as string;

        groupStack.push({ level: groupStack.length, lines: [], name: groupName });

        return;
      }

      if (text.includes("::endgroup::")) {
        const finishedGroup = groupStack.pop();

        if (finishedGroup) {
          const groupElement = (
            <Box key={finishedGroup.name} mb={2} pl={finishedGroup.level * 2}>
              <chakra.details open={open} w="100%">
                <chakra.summary data-testid={`summary-${finishedGroup.name}`}>
                  <chakra.span color="fg.info" cursor="pointer">
                    {finishedGroup.name}
                  </chakra.span>
                </chakra.summary>
                {finishedGroup.lines}
              </chakra.details>
            </Box>
          );

          const lastGroup = groupStack[groupStack.length - 1];

          if (groupStack.length > 0 && lastGroup) {
            lastGroup.lines.push(groupElement);
          } else {
            result.push(groupElement);
          }
        }

        return;
      }

      if (groupStack.length > 0 && groupStack[groupStack.length - 1]) {
        groupStack[groupStack.length - 1]?.lines.push(line);
      } else {
        result.push(line);
      }
    });

    while (groupStack.length > 0) {
      const unfinished = groupStack.pop();

      if (unfinished) {
        result.push(
          <Box key={unfinished.name} mb={2} pl={unfinished.level * 2}>
            {unfinished.lines}
          </Box>,
        );
      }
    }

    return result;
  })();

  return {
    parsedLogs: parsedLines,
    sources,
    warning,
  };
};

export const useLogs = (
  {
    dagId,
    expanded,
    limit,
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    tryNumber = 1,
  }: Props,
  options?: { enabled?: boolean },
) => {
  const { t: translate } = useTranslation("common");
  const [rawLines, setRawLines] = useState<Array<string>>([]);
  const [parsedContent, setParsedContent] = useState<TaskInstancesLogResponse["content"]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const enabled = options?.enabled !== false && Boolean(taskInstance);

  useEffect(() => {
    if (!enabled || !taskInstance) {return;}

    const url = new URL(
      `/api/v2/dags/${taskInstance.dag_id}/dagRuns/${taskInstance.dag_run_id}/taskInstances/${taskInstance.task_id}/logs/${tryNumber}`,
      window.location.origin,
    );

    url.searchParams.set("map_index", String(taskInstance.map_index ?? -1));

    const controller = new AbortController();

    setRawLines([]);
    setParsedContent([]);
    setIsLoading(true);
    setError(null);

    const stream = async () => {
      try {
        const response = await fetch(url.toString(), {
          credentials: "include",
          headers: { accept: "application/x-ndjson" },
          signal: controller.signal,
        });

        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let pending: Array<string> = [];

        // Flush accumulated lines to state at most every 200ms
        const flushInterval = setInterval(() => {
          if (pending.length > 0) {
            const toFlush = pending;

            pending = [];
            const parsed = toFlush.map((line) => {
              try { return JSON.parse(line); } catch { return line; }
            });

            setParsedContent((prev) => [...prev, ...parsed]);
          }
        }, 1000);

        try {
          while (true) {
            const { done, value } = await reader.read();

            if (done) {break;}

            buffer += decoder.decode(value, { stream: true });
            const parts = buffer.split("\n");

            buffer = parts.pop()!;

            const newLines = parts.filter((l) => l.trim());

            if (newLines.length > 0) {
              pending.push(...newLines);
            }
          }
        } finally {
          clearInterval(flushInterval);
          // Flush any remaining lines after stream ends
          if (pending.length > 0) {
            setRawLines((prev) => [...prev, ...pending]);
          }
        }
      } catch (error_) {
        if ((error_ as Error).name !== "AbortError") {
          setError(error_ as Error);
        }
      } finally {
        setIsLoading(false);
      }
    };

    stream();

    return () => controller.abort();
  }, [
    enabled,
    taskInstance?.dag_id,
    taskInstance?.dag_run_id,
    taskInstance?.task_id,
    taskInstance?.map_index,
    tryNumber,
  ]);

  const limitedContent = limit && limit > 0 ? parsedContent.slice(-limit) : parsedContent;

  const parsedData = parseLogs({
    data: limitedContent,
    expanded,
    logLevelFilters,
    showSource,
    showTimestamp,
    sourceFilters,
    taskInstance,
    translate,
    tryNumber,
  });

  const fetchedData = { content: parsedContent } as TaskInstancesLogResponse;

  return { error, fetchedData, isLoading, parsedData };
};
