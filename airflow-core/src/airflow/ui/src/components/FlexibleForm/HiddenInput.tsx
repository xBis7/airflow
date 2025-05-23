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
import { VisuallyHidden } from "@chakra-ui/react";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

/** Render a "const" field where user can not change data as hidden */
export const HiddenInput = ({ name }: FlexibleFormElementProps) => {
  const { paramsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;

  return (
    <VisuallyHidden asChild>
      <input
        id={`element_${name}`}
        name={`element_${name}`}
        type="hidden"
        value={JSON.stringify(param.value ?? "")}
      />
    </VisuallyHidden>
  );
};
