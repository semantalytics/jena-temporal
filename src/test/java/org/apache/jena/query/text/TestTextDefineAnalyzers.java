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

package org.apache.jena.query.text;

import static org.junit.Assert.assertTrue;

import java.io.Reader ;
import java.io.StringReader ;

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.query.text.assembler.TemporalAssembler;
import org.apache.jena.rdf.model.Model ;
import org.apache.jena.rdf.model.ModelFactory ;
import org.apache.jena.rdf.model.Resource ;
import org.junit.After ;
import org.junit.Before ;
import org.junit.Test ;

public class TestTextDefineAnalyzers extends AbstractTestDatasetWithTextIndexBase {

    private static final String SPEC_BASE = "http://example.org/spec#";
    private static final String SPEC_ROOT_LOCAL = "lucene_text_dataset";
    private static final String SPEC_ROOT_URI = SPEC_BASE + SPEC_ROOT_LOCAL;
    private static final String SPEC;
    static {
        SPEC = StrUtils.strjoinNL(
                    "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ",
                    "prefix ja:   <http://jena.hpl.hp.com/2005/11/Assembler#> ",
                    "prefix tdb:  <http://jena.hpl.hp.com/2008/tdb#>",
                    "prefix temporal: <http://jena.apache.org/temporal#>",
                    "prefix :     <" + SPEC_BASE + ">",
                    "",
                    "[] ja:loadClass    \"org.apache.jena.query.temporal.TextQuery\" .",
                    "temporal:TextDataset      rdfs:subClassOf   ja:RDFDataset .",
                    "temporal:TextIndexLucene  rdfs:subClassOf   temporal:TextIndex .",

                    ":" + SPEC_ROOT_LOCAL,
                    "    a              temporal:TextDataset ;",
                    "    temporal:dataset   :dataset ;",
                    "    temporal:index     :indexLucene ;",
                    "    .",
                    "",
                    ":dataset",
                    "    a                     tdb:DatasetTDB ;",
                    "    tdb:location          \"--mem--\" ;",
                    "    tdb:unionDefaultGraph true ;",
                    ".",
                    "",
                    ":indexLucene",
                    "    a temporal:TextIndexLucene ;",
                    "    temporal:directory \"mem\" ;",
                    "    temporal:storeValues true ;",
                    "    temporal:analyzer [",
                    "         a temporal:DefinedAnalyzer ;",
                    "         temporal:useAnalyzer :configuredAnalyzer ] ;",
                    "    temporal:defineAnalyzers (",
                    "         [ temporal:defineAnalyzer :configuredAnalyzer ;",
                    "           temporal:analyzer [",
                    "                a temporal:ConfigurableAnalyzer ;",
                    "                temporal:tokenizer :ngram ;",
                    "                temporal:filters ( :asciiff temporal:LowerCaseFilter ) ] ]",
                    "         [ temporal:defineAnalyzer :configuredAnalyzer2 ;",
                    "           temporal:analyzer [",
                    "                a temporal:ConfigurableAnalyzer ;",
                    "                temporal:tokenizer :ngram2 ;",
                    "                temporal:filters ( :asciiff2 temporal:LowerCaseFilter ) ] ]",
                    "         [ temporal:defineTokenizer :ngram ;",
                    "           temporal:tokenizer [",
                    "                a temporal:GenericTokenizer ;",
                    "                temporal:class \"org.apache.lucene.analysis.ngram.NGramTokenizer\" ;",
                    "                temporal:params (",
                    "                     [ temporal:paramName \"minGram\" ;",
                    "                      temporal:paramType temporal:TypeInt ;",
                    "                       temporal:paramValue 3 ]",
                    "                     [ temporal:paramName \"maxGram\" ;",
                    "                       temporal:paramType temporal:TypeInt ;",
                    "                       temporal:paramValue 7 ]",
                    "                     ) ] ]",
                    "         [ temporal:defineFilter :asciiff ;",
                    "           temporal:filter [",
                    "                a temporal:GenericFilter ;",
                    "                temporal:class \"org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter\" ;",
                    "                temporal:params (",
                    "                     [ temporal:paramName \"preserveOriginal\" ;",
                    "                       temporal:paramType temporal:TypeBoolean ;",
                    "                       temporal:paramValue true ]",
                    "                     ) ] ]",
                    "         [ temporal:defineTokenizer :ngram2 ;",
                    "           temporal:tokenizer [",
                    "                a temporal:GenericTokenizer ;",
                    "                temporal:class \"org.apache.lucene.analysis.ngram.NGramTokenizer\" ;",
                    "                temporal:params (",
                    "                     [ temporal:paramValue 3 ]",
                    "                     [ temporal:paramValue 7 ]",
                    "                     ) ] ]",
                    "         [ temporal:defineFilter :asciiff2 ;",
                    "           temporal:filter [",
                    "                a temporal:GenericFilter ;",
                    "                temporal:class \"org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter\" ;",
                    "                temporal:params (",
                    "                     [ temporal:paramName \"preserveOriginal\" ;",
                    "                       temporal:paramValue true ]",
                    "                     ) ] ]",
                    "         ) ;",
                    "    temporal:entityMap :entMap ;",
                    "    .",
                    "",
                    ":entMap",
                    "    a temporal:EntityMap ;",
                    "    temporal:entityField      \"uri\" ;",
                    "    temporal:defaultField     \"label\" ;",
                    "    temporal:langField        \"lang\" ;",
                    "    temporal:graphField       \"graph\" ;",
                    "    temporal:map (",
                    "         [ temporal:field \"label\" ; temporal:predicate rdfs:label ]",
                    "         [ temporal:field \"comment\" ; temporal:predicate rdfs:comment ]",
                    "         ) ."
                    );
    }

    @Before
    public void before() {
        Reader reader = new StringReader(SPEC);
        Model specModel = ModelFactory.createDefaultModel();
        specModel.read(reader, "", "TURTLE");
        TemporalAssembler.init();
        Resource root = specModel.getResource(SPEC_ROOT_URI);
        dataset = (Dataset) Assembler.general.open(root);
    }

    @After
    public void after() {
        dataset.close();
    }

    private void putTurtleInModel(String turtle, String modelName) {
        Model model = modelName != null ? dataset.getNamedModel(modelName) : dataset.getDefaultModel() ;
        Reader reader = new StringReader(turtle) ;
        dataset.begin(ReadWrite.WRITE) ;
        try {
            model.read(reader, "", "TURTLE") ;
            dataset.commit() ;
        }
        finally {
            dataset.end();
        }
    }

    @Test
    public void testTextQueryDefAnalyzers1() {
        final String turtleA = StrUtils.strjoinNL(
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "testResultOneInModelA>",
                "  rdfs:label 'bar testResultOne barfoo foo'",
                ".",
                "<" + RESOURCE_BASE + "testResultTwoInModelA>",
                "  rdfs:label 'bar testResultTwo barfoo foo'",
                ".",
                "<" + RESOURCE_BASE + "testResultThreeInModelA>",
                "  rdfs:label 'bar testResultThree barfoo foo'",
                "."
                );
        putTurtleInModel(turtleA, "http://example.org/modelA") ;
        final String turtleB = StrUtils.strjoinNL(
                TURTLE_PROLOG,
                "<" + RESOURCE_BASE + "testResultOneInModelB>",
                "  rdfs:label 'bar testResultOne barfoo foo'",
                "."
                );
        putTurtleInModel(turtleB, "http://example.org/modelB") ;
        
        // execution reaches here in the event that the assembler machinery
        // has executed without errors and generated a usable dataset
        // usage of the runtime machinery is tested elsewhere
        assertTrue(true);
    }
}
