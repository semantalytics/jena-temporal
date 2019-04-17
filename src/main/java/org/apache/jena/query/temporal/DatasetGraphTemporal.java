/**
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

package org.apache.jena.query.temporal ;

import java.util.Iterator ;
import java.util.List ;

import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.apache.jena.dboe.transaction.txn.TransactionCoordinator;
import org.apache.jena.dboe.transaction.txn.TransactionalComponent;
import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.query.TxnType;
import org.apache.jena.query.text.TemporalDocProducer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphMonitor;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.tdb.transaction.TransactionManager;
import org.apache.lucene.queryparser.classic.QueryParserBase ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class DatasetGraphTemporal extends DatasetGraphMonitor implements Transactional
{
    private static Logger       log = LoggerFactory.getLogger(DatasetGraphTemporal.class) ;
    private final TemporalIndex temporalIndex;
    private final Graph         dftGraph ;
    private final boolean       closeIndexOnClose;
    // Lock needed for commit/abort that perform an index operation and a dataset operation
    // when the underlying datsetGraph does not coordinate the commit.
    private final Object        txnExitLock = new Object();
    
    // If we are going to implement Transactional, then we are going to have to do as DatasetGraphWithLock and
    // TDB's DatasetGraphTransaction do and track transaction state in a ThreadLocal
    private final ThreadLocal<ReadWrite> readWriteMode = new ThreadLocal<>();
    
    private Runnable delegateCommit = ()-> {
        super.commit();
    };
    
    private Runnable delegateAbort = ()-> {
        super.abort();
    };
    
    private Runnable nonDelegatedCommit = ()-> {
        if (readWriteMode.get() == ReadWrite.WRITE)
            commit_W();
        else
            commit_R();
    };
    
    private Runnable nonDelegatedAbort = ()-> {
        if (readWriteMode.get() == ReadWrite.WRITE)
            abort_W();
        else
            abort_R();
    };

    private Runnable commitAction = null;
    private Runnable abortAction = null;
    
    public DatasetGraphTemporal(DatasetGraph dsg, TemporalIndex index, TemporalDocProducer producer) {
        this(dsg, index, producer, false);
    }

    public DatasetGraphTemporal(DatasetGraph dsg, TemporalIndex index, TemporalDocProducer producer, boolean closeIndexOnClose) {
        super(dsg, producer) ;
        this.temporalIndex = index ;
        dftGraph = GraphView.createDefaultGraph(this) ;
        this.closeIndexOnClose = closeIndexOnClose;
        
        if ( org.apache.jena.tdb.sys.TDBInternal.isTDB1(dsg) ) {
            TransactionManager txnMgr = org.apache.jena.tdb.sys.TDBInternal.getTransactionManager(dsg);
            txnMgr.addAdditionComponent(new TemporalIndexTDB1(temporalIndex));
            commitAction = delegateCommit;
            abortAction = delegateAbort;
        } else if ( org.apache.jena.tdb2.sys.TDBInternal.isTDB2(dsg) ) {
            TransactionCoordinator coord = org.apache.jena.tdb2.sys.TDBInternal.getTransactionCoordinator(dsg);
            // Does not overlap with the ids used by TDB2.
            byte[] componentID = { 2,4,6,10 } ;
            TransactionalComponent tc = new TemporalIndexDB(ComponentId.create(null, componentID), temporalIndex);
            coord.modify(()->coord.add(tc));
            commitAction = delegateCommit;
            abortAction = delegateAbort;
        } else {
            commitAction = nonDelegatedCommit;
            abortAction = nonDelegatedAbort;
        }
    }

    // ---- Intercept these and force the use of views.
    @Override
    public Graph getDefaultGraph() {
        return dftGraph ;
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return GraphView.createNamedGraph(this, graphNode) ;
    }

    // ----

    public TemporalIndex getTemporalIndex() {
        return temporalIndex;
    }

    /** Search the temporal index on the default temporal field */
    public Iterator<TemporalHit> search(String queryString) {
        return search(queryString, null) ;
    }

    /** Search the temporal index on the temporal field associated with the predicate */
    public Iterator<TemporalHit> search(String queryString, Node predicate) {
        return search(queryString, predicate, -1) ;
    }

    /** Search the temporal index on the default temporal field */
    public Iterator<TemporalHit> search(String queryString, int limit) {
        return search(queryString, null, limit) ;
    }

    /** Search the temporal index on the temporal field associated with the predicate */
    public Iterator<TemporalHit> search(String queryString, Node predicate, int limit) {
        return search(queryString, predicate, null, null, limit) ;
    }

    /** Search the temporal index on the temporal field associated with the predicate within graph */
    public Iterator<TemporalHit> search(String queryString, Node predicate, String graphURI, String lang, int limit) {
        queryString = QueryParserBase.escape(queryString) ;
        if ( predicate != null ) {
            String f = temporalIndex.getDocDef().getField(predicate) ;
            queryString = f + ":" + queryString ;
        }
        List<TemporalHit> results = temporalIndex.query(predicate, queryString, graphURI, lang, limit) ;
        return results.iterator() ;
    }

    @Override
    public void begin(TxnType txnType) {
        switch(txnType) {
            case READ_PROMOTE:
            case READ_COMMITTED_PROMOTE:
                throw new UnsupportedOperationException("begin("+txnType+")");
            default:
        }
        begin(TxnType.convert(txnType));
    }
    
    @Override
    public void begin(ReadWrite readWrite) {
        // Do not synchronized(txnLock) here. It will deadlock because if there
        // is an writer in commit, it can't 
        
        // The "super.begin" is enough.
        readWriteMode.set(readWrite);
        super.begin(readWrite) ;
        super.getMonitor().start() ;
    }
    
    @Override
    public void commit() {
        super.getMonitor().finish() ;
        commitAction.run();
        readWriteMode.set(null);
    }
    
    
    /**
     * Rollback all changes, discarding any exceptions that occur.
     */
    @Override
    public void abort() {
        super.getMonitor().finish() ;
        abortAction.run();
        readWriteMode.set(null);
    }

    private void commit_R() {
        // No index action needed.
        super.commit();
    }

    private void commit_W() {
        synchronized(txnExitLock) {
            super.getMonitor().finish() ;
            // Phase 1
            try { temporalIndex.prepareCommit(); }
            catch (Throwable t) {
                log.error("Exception in prepareCommit: " + t.getMessage(), t) ;
                abort();
                throw new TemporalIndexException(t);
            }
            
            // Phase 2
            try {
                // JENA-1302: This needs the exclusive lock for flushing the queue.
                // TDB1
                // Thread 1(W) is running, holds the exclusivitylock=R
                
                // Thread 2(W) starts, tries to commit
                //   Takes txnExitLock
                //   Calls super.commit
                //     Find an excessive flush queue.
                //     It tries to TransactionManger.exclusiveFlushQueue
                //       This needs exclusivitylock=W
                //       So Thread 2 blocks, waiting for thread 1
                //       but still holds txnExitLock
                //
                // Thread 1 tries to commit. 
                //   Can't take the txnExitLock because of thread 2.
                //
                // ==> Deadlock.
                // Fix:
                //   Put index commit into TDB TransactionLifecycle.
                //   No txnExitLock.
                
                // Doing a non-blocking exclusive attempt in TransactionManger.exclusiveFlushQueue
                // does not help - we are in a situation where the queue is growing and unflushable
                // which is why we entered emergency measures. Eventually, RAM will run out as well as
                // the system becoming slow due to Journal layers. 
                
                // TDB2
                //   All work takes place on the W commiting thread.
                //   There is no pause point so this can't happen.
                //     txnExitLock isn't needed, the overall TDB2 transaction 
                //     means W is unique and all work happens without any potential blocking.
                
                super.commit();
                temporalIndex.commit();
            }
            catch (Throwable t) {
                log.error("Exception in commit: " + t.getMessage(), t) ;
                abort();
                throw new TemporalIndexException(t);
            }
        }
    }

    private void abort_R() {
        try { super.abort() ; }
        catch (Throwable t) { log.warn("Exception in abort: " + t.getMessage(), t); }
    }
    
    private void abort_W() {
        synchronized(txnExitLock) {
            // Roll back on both objects, discarding any exceptions that occur
            try { super.abort(); } catch (Throwable t) { log.warn("Exception in abort: " + t.getMessage(), t); }
            try { temporalIndex.rollback(); } catch (Throwable t) { log.warn("Exception in abort: " + t.getMessage(), t); }
        }
    }

    @Override
    public boolean isInTransaction() {
        return readWriteMode.get() != null;
    }

    @Override
    public void end() {
        if ( ! isInTransaction() ) {
            super.end() ;
            return;
        }
        if (readWriteMode.get() == ReadWrite.WRITE) {
            // If we are still in a write transaction at this point, then commit
            // was never called, so rollback the TextIndex and the dataset.
            abortAction.run();
        }
        super.end() ;
        super.getMonitor().finish() ;
        readWriteMode.set(null) ;
    }
    
    @Override
    public boolean supportsTransactions() {
        return super.supportsTransactions() ;
    }
    
    /** Declare whether {@link #abort} is supported.
     *  This goes along with clearing up after exceptions inside application transaction code.
     */
    @Override
    public boolean supportsTransactionAbort() {
        return super.supportsTransactionAbort() ;
    }
    
    @Override
    public void close() {
        super.close();
        if (closeIndexOnClose) {
            temporalIndex.close();
        }
    }
}
