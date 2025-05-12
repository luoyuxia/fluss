/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
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

package com.alibaba.fluss.lake.paimon.tiering;

import org.apache.paimon.table.sink.CommitMessage;

import java.io.Serializable;

/** The write result of Paimon lake writer to pass to commiter to commit. */
public class PaimonWriteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CommitMessage commitMessage;

    public PaimonWriteResult(CommitMessage commitMessage) {
        this.commitMessage = commitMessage;
    }

    public CommitMessage commitMessage() {
        return commitMessage;
    }
}
