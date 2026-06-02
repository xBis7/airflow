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
import { Box, Button } from "@chakra-ui/react";
import type { KeyboardEvent, MouseEvent } from "react";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";

import type { UIAlert } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Alert } from "src/components/ui";
import { alertExpandedKey } from "src/constants/localStorage";

const HASH_MOD = 2_147_483_647;

const hashText = (text: string): string => {
  let hash = 0;

  for (let index = 0; index < text.length; index += 1) {
    hash = (hash * 31 + text.charCodeAt(index)) % HASH_MOD;
  }

  return hash.toString(36);
};

type Props = {
  readonly alert: UIAlert;
};

export const CollapsibleAlert = ({ alert }: Props) => {
  const { t: translate } = useTranslation("dashboard");
  const threshold = alert.collapse_threshold;
  const isCollapsible = threshold !== null && threshold !== undefined && alert.text.length > threshold;
  const [expanded, setExpanded] = useLocalStorage(alertExpandedKey(hashText(alert.text)), false);
  const clamp = isCollapsible && !expanded;

  const toggle = (event: KeyboardEvent | MouseEvent) => {
    event.stopPropagation();
    setExpanded(!expanded);
  };

  const onKeyDown = (event: KeyboardEvent) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      toggle(event);
    }
  };

  return (
    <Alert status={alert.category}>
      <Box flex="1" minWidth={0}>
        <Box
          css={
            clamp
              ? {
                  display: "-webkit-box",
                  overflow: "hidden",
                  WebkitBoxOrient: "vertical",
                  WebkitLineClamp: alert.collapsed_lines,
                }
              : undefined
          }
        >
          <ReactMarkdown>{alert.text}</ReactMarkdown>
        </Box>
        {isCollapsible ? (
          <Button
            fontSize="xs"
            fontWeight="medium"
            height="auto"
            minWidth={0}
            mt={1}
            onClick={toggle}
            onKeyDown={onKeyDown}
            padding={0}
            textDecoration="underline"
            variant="plain"
          >
            {expanded ? translate("alert.showLess") : translate("alert.showMore")}
          </Button>
        ) : undefined}
      </Box>
    </Alert>
  );
};
