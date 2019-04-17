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

package org.apache.jena.query.text;

import org.apache.jena.query.text.assembler.TestEntityMapAssembler;
import org.apache.jena.query.text.assembler.TestTemporalIndexImplAssembler;
import org.apache.jena.query.text.assembler.TestTemporalDatasetAssembler;
import org.apache.jena.query.text.assembler.TestGenericAnalyzerAssembler;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({

    TestBuildTextDataset.class
    , TestDatasetWithLuceneTemporalIndex.class
    , TestDatasetWithLuceneMultilingualTemporalIndex.class
    , TestDatasetWithLuceneTemporalIndexWithLangField.class
    , TestDatasetWithLuceneGraphTemporalIndex.class
    , TestDatasetWithLuceneTemporalIndexDeletionSupport.class
    , TestDatasetWithLuceneStoredLiterals.class
    
    , TestTextNonTxn.class
    , TestTextTxn.class
    , TestTextNonTxnTDB1.class
    , TestTextTxnTDB.class

    , TestEntityMapAssembler.class
    , TestTemporalDatasetAssembler.class
    , TestTemporalIndexImplAssembler.class
    , TestDatasetWithSimpleAnalyzer.class
    , TestDatasetWithStandardAnalyzer.class
    , TestDatasetWithKeywordAnalyzer.class
    , TestDatasetWithLowerCaseKeywordAnalyzer.class
    , TestLuceneWithMultipleThreads.class
    , TestDatasetWithLocalizedAnalyzer.class
    , TestDatasetWithConfigurableAnalyzer.class
    , TestDatasetWithAnalyzingQueryParser.class
    , TestDatasetWithComplexPhraseQueryParser.class
    , TestGenericAnalyzerAssembler.class
    , TestTextGraphIndexExtra.class
    , TestTextGraphIndexExtra2.class
    , TestTextHighlighting.class
    , TestTextDefineAnalyzers.class
    , TestTextMultilingualEnhancements.class
})

public class TS_Text
{}
