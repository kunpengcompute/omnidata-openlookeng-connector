/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package com.huawei.boostkit.omnidata.model;

import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AggregationInfo {
    public AggregationInfo(Map<String, AggregateFunction> aggregations , List<RowExpression> groupingKeys) {
    }

    public Map<String, AggregateFunction> getAggregations() {
        return Collections.emptyMap();
    }

    public List<RowExpression> getGroupingKeys() {
        return Collections.emptyList();
    }

    public static class AggregateFunction {
        public AggregateFunction(CallExpression callExpression, boolean isDistinct) {
        }

        public CallExpression getCall() {
            return null;
        }
        public boolean isDistinct() {
            return false;
        }
    }
}
