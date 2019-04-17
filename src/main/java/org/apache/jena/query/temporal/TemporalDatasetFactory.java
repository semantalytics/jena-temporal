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

package org.apache.jena.query.temporal;

import org.apache.jena.query.Dataset ;
import org.apache.jena.query.DatasetFactory ;
import org.apache.jena.query.temporal.assembler.TemporalVocab;
import org.apache.jena.query.text.TemporalDocProducer;
import org.apache.jena.query.text.TemporalDocProducerTriples;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.assembler.AssemblerUtils ;
import org.apache.jena.sparql.util.Context ;
import org.apache.jena.sys.JenaSystem ;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory ;

public class TemporalDatasetFactory
{
    static { JenaSystem.init(); }
    
    /** Use an assembler file to build a dataset with temporal search capabilities */
    public static Dataset create(String assemblerFile)
    {
        return (Dataset)AssemblerUtils.build(assemblerFile, TemporalVocab.temporalDataset) ;
    }

    /** Create a temporal-indexed dataset */
    public static Dataset create(Dataset base, org.apache.jena.query.temporal.TemporalIndex temporalIndex)
    {
        return create(base, temporalIndex, false);
    }
    
    /** Create a temporal-indexed dataset, optionally allowing the temporal index to be closed if the Dataset is */
    public static Dataset create(Dataset base, org.apache.jena.query.temporal.TemporalIndex temporalIndex, boolean closeIndexOnDSGClose)
    {
        DatasetGraph dsg = base.asDatasetGraph() ;
        dsg = create(dsg, temporalIndex, closeIndexOnDSGClose) ;
        return DatasetFactory.wrap(dsg) ;
    }
    
    /** Create a temporal-indexed dataset, optionally allowing the temporal index to be closed if the Dataset is */
    public static Dataset create(Dataset base, org.apache.jena.query.temporal.TemporalIndex temporalIndex, boolean closeIndexOnDSGClose, TemporalDocProducer producer)
    {
        DatasetGraph dsg = base.asDatasetGraph() ;
        dsg = create(dsg, temporalIndex, closeIndexOnDSGClose, producer) ;
        return DatasetFactory.wrap(dsg) ;
    }


    /** Create a temporal-indexed DatasetGraph */
    public static DatasetGraph create(DatasetGraph dsg, org.apache.jena.query.temporal.TemporalIndex temporalIndex)
    {
        return create(dsg, temporalIndex, false);
    }
    
    /** Create a temporal-indexed DatasetGraph, optionally allowing the temporal index to be closed if the DatasetGraph is */
    public static DatasetGraph create(DatasetGraph dsg, org.apache.jena.query.temporal.TemporalIndex temporalIndex, boolean closeIndexOnDSGClose)
    {
        return create(dsg, temporalIndex, closeIndexOnDSGClose, null);
    }
    
    /** Create a temporal-indexed DatasetGraph, optionally allowing the temporal index to be closed if the DatasetGraph is */
    public static DatasetGraph create(DatasetGraph dsg, org.apache.jena.query.temporal.TemporalIndex temporalIndex, boolean closeIndexOnDSGClose, TemporalDocProducer producer) {
        if (producer == null) producer = new TemporalDocProducerTriples(temporalIndex) ;
        DatasetGraph dsgt = new DatasetGraphTemporal(dsg, temporalIndex, producer, closeIndexOnDSGClose) ;
        // Also set on dsg
        Context c = dsgt.getContext() ;
        c.set(TemporalQuery.textIndex, temporalIndex) ;
        
        return dsgt ;
    }

    /**
     * Create a Lucene TextIndex
     *
     * @param directory The Lucene Directory for the index
     * @param def The EntityDefinition that defines how entities are stored in the index
     * @param queryAnalyzer The analyzer to be used to find terms in the query temporal.  If null, then the analyzer defined by the EntityDefinition will be used.
     */
    public static org.apache.jena.query.temporal.TemporalIndex createLuceneIndex(Directory directory, EntityDefinition def, Analyzer queryAnalyzer)
    {
        TemporalIndexConfig config = new TemporalIndexConfig(def);
        config.setQueryAnalyzer(queryAnalyzer);
        return createLuceneIndex(directory, config);
    }

    /**
     * Create a Lucene TextIndex
     *
     * @param directory The Lucene Directory for the index
     * @param config The config definition for the index instantiation.
     */
    public static org.apache.jena.query.temporal.TemporalIndex createLuceneIndex(Directory directory, TemporalIndexConfig config)
    {
        return new TemporalIndexImpl(directory, config) ;
    }

    /**
     * Create a temporal-indexed dataset, using Lucene
     *
     * @param base the base Dataset
     * @param directory The Lucene Directory for the index
     * @param def The EntityDefinition that defines how entities are stored in the index
     * @param queryAnalyzer The analyzer to be used to find terms in the query temporal.  If null, then the analyzer defined by the EntityDefinition will be used.
     */
    public static Dataset createLucene(Dataset base, Directory directory, EntityDefinition def, Analyzer queryAnalyzer)
    {
        TemporalIndexConfig config = new TemporalIndexConfig(def);
        config.setQueryAnalyzer(queryAnalyzer);
        return createLucene(base, directory, config);
    }

    /**
     * Create a temporal-indexed dataset, using Lucene
     *
     * @param base the base Dataset
     * @param directory The Lucene Directory for the index
     * @param config The config definition for the index instantiation.
     */
    public static Dataset createLucene(Dataset base, Directory directory, TemporalIndexConfig config)
    {
        org.apache.jena.query.temporal.TemporalIndex index = createLuceneIndex(directory, config) ;
        return create(base, index, true) ;
    }

    /**
     * Create a temporal-indexed dataset, using Lucene
     *
     * @param base the base DatasetGraph
     * @param directory The Lucene Directory for the index
     * @param def The EntityDefinition that defines how entities are stored in the index
     * @param queryAnalyzer The analyzer to be used to find terms in the query temporal.  If null, then the analyzer defined by the EntityDefinition will be used.
     */
    public static DatasetGraph createLucene(DatasetGraph base, Directory directory, EntityDefinition def, Analyzer queryAnalyzer)
    {
        TemporalIndexConfig config = new TemporalIndexConfig(def);
        config.setQueryAnalyzer(queryAnalyzer);
        return createLucene(base, directory, config) ;
    }

    /**
     * Create a temporal-indexed dataset, using Lucene
     *
     * @param base the base DatasetGraph
     * @param directory The Lucene Directory for the index
     * @param config The config definition for the index instantiation.
     */
    public static DatasetGraph createLucene(DatasetGraph base, Directory directory, TemporalIndexConfig config)
    {
        org.apache.jena.query.temporal.TemporalIndex index = createLuceneIndex(directory, config) ;
        return create(base, index, true) ;
    }
}

