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

import org.apache.jena.assembler.Assembler;
import org.apache.jena.query.text.TemporalIndexException;
import org.apache.jena.query.text.analyzer.ConfigurableAnalyzer;
import org.apache.jena.query.text.assembler.GenericTokenizerAssembler.TokenizerSpec;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;

public class DefineTokenizersAssembler {
    /*
    <#indexLucene> a temporal:TextIndexLucene ;
        temporal:directory <file:Lucene> ;
        temporal:entityMap <#entMap> ;
        temporal:defineAnalyzers (
            [temporal:addLang "sa-x-iast" ;
             temporal:analyzer [ . . . ]]
            [temporal:defineAnalyzer <#foo> ;
             temporal:analyzer [ . . . ]]
            [temporal:defineFilter <#bar> ;
             temporal:filter [ . . . ]]
            [temporal:defineTokenizer <#baz> ;
             temporal:tokenizer [ . . . ]]
        )
    */

    public static boolean open(Assembler a, Resource list) {
        Resource current = list;
        boolean isMultilingualSupport = false;
        
        while (current != null && ! current.equals(RDF.nil)){
            Statement firstStmt = current.getProperty(RDF.first);
            if (firstStmt == null) {
                throw new TemporalIndexException("parameter list not well formed: " + current);
            }
            
            RDFNode first = firstStmt.getObject();
            if (! first.isResource()) {
                throw new TemporalIndexException("parameter specification must be an anon resource : " + first);
            }

            // process the current list element to add an analyzer 
            Resource adding = (Resource) first;
            if (adding.hasProperty(TemporalVocab.pTokenizer)) {
                Statement tokenizerStmt = adding.getProperty(TemporalVocab.pTokenizer);
                RDFNode tokenizerNode = tokenizerStmt.getObject();
                if (!tokenizerNode.isResource()) {
                    throw new TemporalIndexException("addTokenizers temporal:tokenizer must be an tokenizer spec resource: " + tokenizerNode);
                }
                
                TokenizerSpec spec = (TokenizerSpec) a.open((Resource) tokenizerNode);
                
                if (adding.hasProperty(TemporalVocab.pDefTokenizer)) {
                    Statement defStmt = adding.getProperty(TemporalVocab.pDefTokenizer);
                    Resource id = defStmt.getResource();
                    
                    if (id.getURI() != null) {
                        ConfigurableAnalyzer.defineTokenizer(id.getURI(), spec);
                    } else {
                        throw new TemporalIndexException("addTokenizers temporal:defineTokenizer property must be a non-blank resource: " + adding);
                    }
                }
            }
            
            Statement restStmt = current.getProperty(RDF.rest);
            if (restStmt == null) {
                throw new TemporalIndexException("parameter list not terminated by rdf:nil");
            }
            
            RDFNode rest = restStmt.getObject();
            if (! rest.isResource()) {
                throw new TemporalIndexException("parameter list node is not a resource : " + rest);
            }
            
            current = (Resource) rest;
        }
        
        return isMultilingualSupport;
    }
}
