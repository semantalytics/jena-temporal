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
import org.apache.jena.query.text.TemporalIndexException;
import org.apache.jena.query.text.analyzer.Util;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.lucene.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefineAnalyzersAssembler {
    /*
    <#indexLucene> a temporal:TextIndexLucene ;
        temporal:directory <file:Lucene> ;
        temporal:entityMap <#entMap> ;
        temporal:defineAnalyzers (
            [temporal:addLang "sa-x-iast" ;
             temporal:analyzer [ . . . ]]
            [temporal:defineAnalyzer <#foo> ;
             temporal:analyzer [ . . . ]]
        )
    */
    private static Logger log = LoggerFactory.getLogger(DefineAnalyzersAssembler.class) ;

    private static List<String> getStringList(Statement stmt, String p) {
        List<String> tags = new ArrayList<String>();
        RDFNode aNode = stmt.getObject();
        if (! aNode.isResource()) {
            throw new TemporalIndexException(p + " property is not a list : " + aNode);
        }

        Resource current = (Resource) aNode;
        while (current != null && ! current.equals(RDF.nil)) {
            Statement firstStmt = current.getProperty(RDF.first);
            if (firstStmt == null) {
                throw new TemporalIndexException(p + " list not well formed: " + current);
            }

            RDFNode first = firstStmt.getObject();
            if (! first.isLiteral()) {
                throw new TemporalIndexException(p + " list not a String : " + first);
            }

            String tag = first.toString();
            tags.add(tag);
            
            Statement restStmt = current.getProperty(RDF.rest);
            if (restStmt == null) {
                throw new TemporalIndexException(p + " list not terminated by rdf:nil");
            }
            
            RDFNode rest = restStmt.getObject();
            if (! rest.isResource()) {
                throw new TemporalIndexException(p + " list rest node is not a resource : " + rest);
            }
            
            current = (Resource) rest;
        }
       
        return tags;
    }
   
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
            if (adding.hasProperty(TemporalVocab.pAnalyzer)) {
                Statement analyzerStmt = adding.getProperty(TemporalVocab.pAnalyzer);
                RDFNode analyzerNode = analyzerStmt.getObject();
                if (!analyzerNode.isResource()) {
                    throw new TemporalIndexException("addAnalyzers temporal:analyzer must be an analyzer spec resource: " + analyzerNode);
                }
                
                // calls GenericAnalyzerAssembler
                Analyzer analyzer = (Analyzer) a.open((Resource) analyzerNode);
                
                if (adding.hasProperty(TemporalVocab.pDefAnalyzer)) {
                    Statement defStmt = adding.getProperty(TemporalVocab.pDefAnalyzer);
                    Resource id = defStmt.getResource();
                    
                    if (id.getURI() != null) {
                        Util.defineAnalyzer(id, analyzer);
                    } else {
                        throw new TemporalIndexException("addAnalyzers temporal:defineAnalyzer property must be a non-blank resource: " + adding);
                    }
                }
                
                String langCode = null;
                
                if (adding.hasProperty(TemporalVocab.pAddLang)) {
                    Statement langStmt = adding.getProperty(TemporalVocab.pAddLang);
                    langCode = langStmt.getString();
                    Util.addAnalyzer(langCode, analyzer);
                    isMultilingualSupport = true;
                }
                
                if (langCode != null && adding.hasProperty(TemporalVocab.pSearchFor)) {
                    Statement searchForStmt = adding.getProperty(TemporalVocab.pSearchFor);
                    List<String> tags = getStringList(searchForStmt, "temporal:searchFor");
                    Util.addSearchForTags(langCode, tags);
                }
                
                if (langCode != null && adding.hasProperty(TemporalVocab.pAuxIndex)) {
                    Statement searchForStmt = adding.getProperty(TemporalVocab.pAuxIndex);
                    List<String> tags = getStringList(searchForStmt, "temporal:auxIndex");
                    Util.addAuxIndexes(langCode, tags);
                    log.trace("addAuxIndexes for {} with tags: {}", langCode, tags);
                }
                               
                if (adding.hasProperty(TemporalVocab.pIndexAnalyzer)) {
                    Statement indexStmt = adding.getProperty(TemporalVocab.pIndexAnalyzer);
                    Resource key = indexStmt.getResource();
                    Analyzer indexer = Util.getDefinedAnalyzer(key);
                    Util.addIndexAnalyzer(langCode, indexer);
                    log.trace("addIndexAnalyzer lang: {} with analyzer: {}", langCode, indexer);
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
