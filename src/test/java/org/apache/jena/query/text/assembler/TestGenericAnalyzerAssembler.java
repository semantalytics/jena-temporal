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

package org.apache.jena.query.text.assembler;

import static org.junit.Assert.assertEquals;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.vocabulary.RDF;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.junit.Test;

public class TestGenericAnalyzerAssembler {

    private static final Resource spec1;
    private static final Resource spec2;
    private static final Resource spec3;
    private static final Resource spec4;
    private static final Resource spec5;
    private static final Resource spec6;
    
    @Test public void AnalyzerNullaryCtor() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec1, null);
        assertEquals(SimpleAnalyzer.class, analyzer.getClass());
    }
    
    @Test public void AnalyzerNullaryCtor2() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec2, null);
        assertEquals(FrenchAnalyzer.class, analyzer.getClass());
    }
    
    @Test public void AnalyzerCtorSet1() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec3, null);
        assertEquals(FrenchAnalyzer.class, analyzer.getClass());
    }
    
    @Test public void AnalyzerCtorAnalyzerInt() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec4, null);
        assertEquals(ShingleAnalyzerWrapper.class, analyzer.getClass());
    }
    
    @Test public void AnalyzerCtorShingle7() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec5, null);
        assertEquals(ShingleAnalyzerWrapper.class, analyzer.getClass());
    }
    
    @Test public void AnalyzerCtorFile() {
        GenericAnalyzerAssembler gaAssem = new GenericAnalyzerAssembler();
        Analyzer analyzer = gaAssem.open(null, spec6, null);
        assertEquals(StopAnalyzer.class, analyzer.getClass());
    }
    
    
    private static final String CLASS_SIMPLE = "org.apache.lucene.analysis.core.SimpleAnalyzer";
    private static final String CLASS_FRENCH = "org.apache.lucene.analysis.fr.FrenchAnalyzer";
    private static final String CLASS_SHINGLE = "org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper";
    private static final String CLASS_STOP = "org.apache.lucene.analysis.core.StopAnalyzer";
    
    private static final String FILE_STOPS = "testing/some-stop-words.txt";
    
    static {
        JenaSystem.init();
        TemporalAssembler.init();
        Model model = ModelFactory.createDefaultModel();
        
        // analyzer spec w/ no params
                
        spec1 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_SIMPLE)
                     ;
        
        // analyzer spec w/ empty params
                
        spec2 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_FRENCH)
                     .addProperty(TemporalVocab.pParams,
                                  model.createList(
                                          new RDFNode[] { } )
                                  )
                     ;
        
        // analyzer spec w/ one set param
                
        spec3 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_FRENCH)
                     .addProperty(TemporalVocab.pParams,
                                  model.createList(
                                          new RDFNode[] { 
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "stopWords")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeSet)
                                                  .addProperty(TemporalVocab.pParamValue, strs2list(model, "les le du"))
                                          }))
                     ;
        
        // analyzer spec w/ analyzer param and int
                
        spec4 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_SHINGLE)
                     .addProperty(TemporalVocab.pParams,
                                  model.createList(
                                          new RDFNode[] { 
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "defaultAnalyzer")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeAnalyzer)
                                                  .addProperty(TemporalVocab.pParamValue,
                                                               model.createResource()
                                                               .addProperty(RDF.type, TemporalVocab.simpleAnalyzer)
                                                               ),
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "maxShingleSize")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeInt)
                                                  .addLiteral(TemporalVocab.pParamValue, 3)
                                          }))
                     ;
        
        // analyzer spec w/ seven params of mixed types
                
        spec5 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_SHINGLE)
                     .addProperty(TemporalVocab.pParams,
                                  model.createList(
                                          new RDFNode[] { 
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "delegate")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeAnalyzer)
                                                  .addProperty(TemporalVocab.pParamValue,
                                                               model.createResource()
                                                               .addProperty(RDF.type, TemporalVocab.simpleAnalyzer)
                                                               ) ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "minShingleSize")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeInt)
                                                  .addLiteral(TemporalVocab.pParamValue, 2) ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "maxShingleSize")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeInt)
                                                  .addLiteral(TemporalVocab.pParamValue, 4) ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "tokenSeparator")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeString)
                                                  .addLiteral(TemporalVocab.pParamValue, "|") ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "outputUnigrams")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeBoolean)
                                                  .addLiteral(TemporalVocab.pParamValue, false) ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "outputUnigramsIfNoShingles")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeBoolean)
                                                  .addLiteral(TemporalVocab.pParamValue, true) ,
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "fillerToken")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeString)
                                                  .addLiteral(TemporalVocab.pParamValue, "foo")
                                          }))
                     ;
        
        // analyzer spec w/ one file param
                
        spec6 = model.createResource()
                     .addProperty(RDF.type, TemporalVocab.genericAnalyzer)
                     .addProperty(TemporalVocab.pClass, CLASS_STOP)
                     .addProperty(TemporalVocab.pParams,
                                  model.createList(
                                          new RDFNode[] { 
                                                  model.createResource()
                                                  .addProperty(TemporalVocab.pParamName, "stopWords")
                                                  .addProperty(TemporalVocab.pParamType, TemporalVocab.typeFile)
                                                  .addProperty(TemporalVocab.pParamValue, FILE_STOPS)
                                          }))
                     ;
    }
    
    private static Resource strs2list(Model model, String string) {
        String[] members = string.split("\\s");
        Resource current = RDF.nil;
        for (int i = members.length-1; i>=0; i--) {
            Resource previous = current;
            current = model.createResource();
            current.addProperty(RDF.rest, previous);
            current.addProperty(RDF.first, members[i]);            
        }
        return current;    
    }
}
