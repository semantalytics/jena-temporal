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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.query.temporal.TemporalIndexException;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement ;
import org.apache.jena.vocabulary.RDF ;
import org.apache.lucene.analysis.Analyzer;


/**
 * Assembler to create a configurable analyzer.
 */
public class ConfigurableAnalyzerAssembler extends AssemblerBase {
    /*
    temporal:map (
         [ temporal:field "temporal" ;
           temporal:predicate rdfs:label;
           temporal:analyzer [
               a  temporal:ConfigurableAnalyzer ;
               temporal:tokenizer temporal:LetterTokenizer ;
               temporal:filters (temporal:LowerCaseFilter)
           ]
         ]
        .
    */


    @Override
    public Analyzer open(Assembler a, Resource root, Mode mode) {
        if (root.hasProperty(TemporalVocab.pTokenizer)) {
            Resource tokenizerResource = root.getPropertyResourceValue(TemporalVocab.pTokenizer);
            String tokenizer = tokenizerResource.getURI();
            List<String> filters;
            if (root.hasProperty(TemporalVocab.pFilters)) {
                Resource filtersResource = root.getPropertyResourceValue(TemporalVocab.pFilters);
                filters = toFilterList(filtersResource);
            } else {
                filters = new ArrayList<>();
            }
            return new ConfigurableAnalyzer(tokenizer, filters);
        } else {
            throw new TemporalIndexException("temporal:tokenizer setting is required by ConfigurableAnalyzer");
        }
    }

    private List<String> toFilterList(Resource list) {
        List<String> result = new ArrayList<>();
        Resource current = list;
        while (current != null && ! current.equals(RDF.nil)){
            Statement stmt = current.getProperty(RDF.first);
            if (stmt == null) {
                throw new TemporalIndexException("filter list not well formed");
            }
            RDFNode node = stmt.getObject();
            if (! node.isResource()) {
                throw new TemporalIndexException("filter is not a resource : " + node);
            }
            
            result.add(node.asResource().getURI());
            stmt = current.getProperty(RDF.rest);
            if (stmt == null) {
                throw new TemporalIndexException("filter list not terminated by rdf:nil");
            }
            node = stmt.getObject();
            if (! node.isResource()) {
                throw new TemporalIndexException("filter list node is not a resource : " + node);
            }
            current = node.asResource();
        }
        return result;
    }

}
