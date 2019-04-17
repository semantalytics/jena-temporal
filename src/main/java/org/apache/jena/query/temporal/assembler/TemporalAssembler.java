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

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.sparql.core.assembler.AssemblerUtils ;

public class TemporalAssembler
{
    public static void init()
    {
        AssemblerUtils.init() ;
        AssemblerUtils.registerDataset(TemporalVocab.temporalDataset, new TemporalDatasetAssembler()) ;
        
        Assembler.general.implementWith(TemporalVocab.entityMap, new EntityDefinitionAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.temporalIndexLucene, new TextIndexLuceneAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.standardAnalyzer, new StandardAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.simpleAnalyzer, new SimpleAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.keywordAnalyzer, new KeywordAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.lowerCaseKeywordAnalyzer, new LowerCaseKeywordAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.localizedAnalyzer, new LocalizedAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.configurableAnalyzer, new ConfigurableAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.genericAnalyzer, new GenericAnalyzerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.genericFilter, new GenericFilterAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.genericTokenizer, new GenericTokenizerAssembler()) ;
        Assembler.general.implementWith(TemporalVocab.definedAnalyzer, new DefinedAnalyzerAssembler()) ;

    }
}

