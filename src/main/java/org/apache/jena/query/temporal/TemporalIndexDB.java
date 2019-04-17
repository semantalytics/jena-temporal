/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.query.temporal;

import java.nio.ByteBuffer;

import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.apache.jena.dboe.transaction.txn.TransactionalComponentBase;
import org.apache.jena.dboe.transaction.txn.TxnId;
import org.apache.jena.query.ReadWrite;

/** 
 * Adapter to put Lucene into DBOE transactions.
 */
public class TemporalIndexDB extends TransactionalComponentBase<TemporalIndexDB.TemporalState> {

    private final TemporalIndex temporalIndex;

    public TemporalIndexDB(ComponentId id, TemporalIndex temporalIndex) {
        super(id);
        this.temporalIndex = temporalIndex;
    }

    static class TemporalState {}

    @Override
    protected TemporalState _begin(ReadWrite readWrite, TxnId txnId) {
        // Need to MRSW?
        return new TemporalState();
    }
    
    @Override
    protected ByteBuffer _commitPrepare(TxnId txnId, TemporalState state) {
        temporalIndex.prepareCommit();
        return null;
    }

    // Check.
    @Override
    protected void _commit(TxnId txnId, TemporalState state) {
        temporalIndex.commit();
    }

    @Override
    protected void _commitEnd(TxnId txnId, TemporalState state) {}

    @Override
    protected void _abort(TxnId txnId, TemporalState state) {
        temporalIndex.rollback();
    }

}
