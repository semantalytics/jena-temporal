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

package org.apache.jena.query.temporal.assembler ;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.assembler.Mode ;
import org.apache.jena.assembler.assemblers.AssemblerBase ;
import org.apache.jena.query.text.TemporalIndexException;
import org.apache.jena.rdf.model.Literal ;
import org.apache.jena.rdf.model.RDFNode ;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.rdf.model.Statement ;
import org.apache.jena.vocabulary.RDF ;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.CharArraySet;

/**
 * Assembler to create standard analyzers with keyword list.
 */
public class StandardAnalyzerAssembler extends AssemblerBase {
    /*
    temporal:map (
         [ temporal:field "temporal" ;
           temporal:predicate rdfs:label;
           temporal:analyzer [
               a  lucene:StandardAnalyzer ;
               temporal:stopWords ("foo" "bar" "baz") # optional
           ]
         ]
        .
     */

    @Override
    public Analyzer open(Assembler a, Resource root, Mode mode) {
        if (root.hasProperty(TextVocab.pStopWords)) {
            return analyzerWithStopWords(root);
        } else {
            return new StandardAnalyzer();
        }
    }

    private Analyzer analyzerWithStopWords(Resource root) {
        RDFNode node = root.getProperty(TextVocab.pStopWords).getObject();
        if (! node.isResource()) {
            throw new TemporalIndexException("temporal:stopWords property takes a list as a value : " + node);
        }
        CharArraySet stopWords = toCharArraySet((Resource) node);
        return new StandardAnalyzer(stopWords);
    }

    private CharArraySet toCharArraySet(Resource list) {
        return new CharArraySet(toList(list), false);
    }

    private List<String> toList(Resource list) {
        List<String> result = new ArrayList<>();
        Resource current = list;
        while (current != null && ! current.equals(RDF.nil)){
            Statement stmt = current.getProperty(RDF.first);
            if (stmt == null) {
                throw new TemporalIndexException("stop word list not well formed");
            }
            RDFNode node = stmt.getObject();
            if (! node.isLiteral()) {
                throw new TemporalIndexException("stop word is not a literal : " + node);
            }
            result.add(((Literal)node).getLexicalForm());
            stmt = current.getProperty(RDF.rest);
            if (stmt == null) {
                throw new TemporalIndexException("stop word list not terminated by rdf:nil");
            }
            node = stmt.getObject();
            if (! node.isResource()) {
                throw new TemporalIndexException("stop word list node is not a resource : " + node);
            }
            current = (Resource) node;
        }
        return result;
    }
}
