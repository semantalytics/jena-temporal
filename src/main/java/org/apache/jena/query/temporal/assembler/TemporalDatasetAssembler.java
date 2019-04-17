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

package org.apache.jena.query.temporal.assembler;

import static org.apache.jena.query.temporal.assembler.TemporalVocab.pTemporalDocProducer;
import static org.apache.jena.query.temporal.assembler.TemporalVocab.pDataset ;
import static org.apache.jena.query.temporal.assembler.TemporalVocab.pIndex ;
import static org.apache.jena.query.temporal.assembler.TemporalVocab.pTemporalDocProducer ;
import static org.apache.jena.query.temporal.assembler.TemporalVocab.temporalDataset ;

import java.lang.reflect.Constructor ;

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.assembler.Mode ;
import org.apache.jena.assembler.assemblers.AssemblerBase ;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.temporal.TemporalDatasetFactory;
import org.apache.jena.query.temporal.TemporalIndex;
import org.apache.jena.query.temporal.TemporalIndex;
import org.apache.jena.query.temporal.TemporalDatasetFactory;
import org.apache.jena.query.text.TemporalDocProducer;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.sparql.ARQConstants ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.util.ClsLoader ;
import org.apache.jena.sparql.util.graph.GraphUtils ;

public class TemporalDatasetAssembler extends AssemblerBase implements Assembler {

    public static Resource getType() {
        return temporalDataset;
    }

    /*
    <#text_dataset>
      rdf:type temporal:Dataset ;
      temporal:dataset <#dataset> ;
      temporal:index   <#index> ;
    .

     */

    @Override
    public Dataset open(Assembler a, Resource root, Mode mode)
    {
        Resource dataset = GraphUtils.getResourceValue(root, pDataset) ;
        Resource index   = GraphUtils.getResourceValue(root, pIndex) ;
        Resource temporalDocProducerNode = GraphUtils.getResourceValue(root, pTemporalDocProducer) ;

        Dataset ds = (Dataset)a.open(dataset) ;
        TemporalIndex temporalIndex = (TemporalIndex)a.open(index) ;
        // Null will use the default producer
        TemporalDocProducer temporalDocProducer = null ;
        if (null != temporalDocProducerNode) {
            Class<?> c = ClsLoader.loadClass(temporalDocProducerNode.getURI(), TemporalDocProducer.class) ;

            String className = temporalDocProducerNode.getURI().substring(ARQConstants.javaClassURIScheme.length()) ;
            Constructor<?> dyadic = getConstructor(c, DatasetGraph.class, TemporalIndex.class);
            Constructor<?> monadic = getConstructor(c, TemporalIndex.class);

            try {
                if (dyadic != null) {
                    temporalDocProducer = (TemporalDocProducer) dyadic.newInstance(ds.asDatasetGraph(), temporalIndex) ;
                } else if (monadic != null) {
                    temporalDocProducer = (TemporalDocProducer) monadic.newInstance(temporalIndex) ;
                } else {
                    Log.warn(ClsLoader.class, "Exception during instantiation '"+className+"' no TextIndex or DatasetGraph,Index constructor" );
                }
            } catch (Exception ex) {
                Log.warn(ClsLoader.class, "Exception during instantiation '"+className+"': "+ex.getMessage()) ;
                return null ;
            }
        }

        Dataset dst = TemporalDatasetFactory.create(ds, temporalIndex, true, temporalDocProducer) ;
        return dst ;
    }

    private static Constructor<?> getConstructor(Class<?> c, Class<?> ...types) {
        try {
            return c.getConstructor(types);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
