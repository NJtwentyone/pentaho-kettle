/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.util;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * Test class for StringEvaluator functionality.
 *
 * @author Alexander Buloichik
 */
public class StringEvaluatorTest {

  private StringEvaluator evaluator;
  private static Locale defaultLocale;

  @BeforeClass
  public static void setUpClass() {
    defaultLocale = Locale.getDefault();
  }

  @Before
  public void setUp() {
    evaluator = new StringEvaluator();
    Locale.setDefault(Locale.US);
  }

  @AfterClass
  public static void tearDown(){
    Locale.setDefault(defaultLocale);
  }

  /////////////////////////////////////
  // common
  ////////////////////////////////////
  @Test
  public void testEmpty() {
    evaluator.evaluateString( "" );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );

    evaluator.evaluateString( "  " );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
  }

  @Test
  public void testGetValues_WithoutDublicate() {
    List<String> strings = Arrays.asList( "name1", "name2" );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertEquals( strings.size(), evaluator.getValues().size() );
    List<String> actualStrings = new ArrayList<String>( evaluator.getValues() );
    Collections.sort( strings );
    Collections.sort( actualStrings );

    Iterator<String> exIterator = strings.iterator();
    Iterator<String> iterator = actualStrings.iterator();
    while ( iterator.hasNext() ) {
      assertEquals( exIterator.next(), iterator.next() );
    }
  }

  @Test
  public void testGetValues_Duplicate() {
    String dublicatedString = "name1";
    List<String> strings = Arrays.asList( dublicatedString );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    evaluator.evaluateString( dublicatedString );
    assertEquals( strings.size(), evaluator.getValues().size() );
    Iterator<String> exIterator = strings.iterator();
    Iterator<String> iterator = evaluator.getValues().iterator();
    while ( iterator.hasNext() ) {
      assertEquals( exIterator.next(), iterator.next() );
    }
  }

  @Test
  public void testGetCount() {
    List<String> strings = Arrays.asList( "02/29/2000", "03/29/2000" );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertEquals( strings.size(), evaluator.getCount() );
  }

  @Test
  public void testGetAdvicedResult_NullInput() {
    String expectedNull = "";
    List<String> strings = Arrays.asList( expectedNull );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals( 1, result.getNrNull() );
  }

  @Test
  public void testGetAdvicedResult_MinMaxInput() {
    String expectedMin = "500";
    String expectedMax = "1000";
    List<String> strings = Arrays.asList( expectedMax, expectedMin );
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    assertEquals( Long.parseLong( expectedMax ), result.getMax() );
    assertEquals( Long.parseLong( expectedMin ), result.getMin() );
    assertEquals( expectedMax.length(), evaluator.getMaxLength() );
  }

  /////////////////////////////////////
  // mixed types
  ////////////////////////////////////
  @Test
  public void testIntegerWithNumber() {
    List<String> strings = Arrays.asList( "1", "1.1" );
    String mask = "#.#";
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isNumber() );
    assertTrue( mask.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  /////////////////////////////////////
  // number types
  ////////////////////////////////////
  @Test
  public void testNumberWithPoint() {
    testNumber( "#.#", "1.1" );
  }

  @Test
  public void testNumberWithGroupAndPoint() {
    testNumber( "#,###,###.#", "1,111,111.1" );
  }

  @Test
  public void testNumbers() {
    testNumber( "#,###,###.#", "1,111,111.1", "1,111" );
  }

  @Test
  public void testStringAsNumber() {
    evaluator.evaluateString( "1" );
    evaluator.evaluateString( "1.111111111" );
    evaluator.evaluateString( "1,111" );
    evaluator.evaluateString( "1,111.11111111" );
    evaluator.evaluateString( "1,111,111.1111" );
    evaluator.evaluateString( "1,111.111.1111" );
    evaluator.evaluateString( "1,111,111.111.111" );
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isString() );
  }

  private void testNumber( String mask, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    StringEvaluationResult sre = evaluator.getAdvicedResult();
    ValueMetaInterface vmi = sre.getConversionMeta();
    assertTrue( vmi.isNumber() );
    assertEquals( mask, vmi.getConversionMask() );
  }

  /////////////////////////////////////
  // integer types
  ////////////////////////////////////
  @Test
  public void testInteger() {
    testInteger( "#", "1" );
  }

  @Test
  public void testIntegerWithGroup() {
    testInteger( "#", "1111" );
  }

  private void testInteger( String mask, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isInteger() );
    assertTrue( mask.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  /** DEBUG
   * Copied some of tim's test from - https://github.com/NJtwentyone/pentaho-kettle/commit/f09f3a283d84179410a6eceb8d81110a2cf3f3e8
   *  WITH CURRENT IMPLEMENTATION ie 9.4.0.0-288 and earlier (start of time,  9.4.0.0-288)
   *  TODO ORDER OF strings matter #checkout how StringEvaluator#challengeConversions removes variables in StringEvaluator.evaluationResults
   *  TODO check out comparator StringEvaluation#getAdvicedResult(), that determines the mask
   *  TODO why can't we keep track of StringEvaluationResult that sets the variable StringEvaluator.maxPrecision in StringEvaluator#evaluatePrecision
   *  TODO verify if there are "requirements" to in code comments '// want the shortest format mask for numerics & integers'
   *  FIXME add length (getConversionMeta().getLength())  and precision (getConversionMeta().getPrecision()) asserts to all tests
   *
   */

  // DEBUG
  @Test
  public void testColumnOfVaryingScale_PDI_19619_precision_3_small_precision_asc() {
    testNumber( "#.000", "1.1", "1.999", "1.23" ); // FAILS - returns mask "#.#"
  }

  // DEBUG
  @Test
  public void testColumnOfVaryingScale_PDI_19619_precision_3_small_precision_desc() {
    testNumber( "#.000", "1.999", "1.23", "1.1" ); // FAILS - returns mask "#.#"
  }

  // DEBUG
  @Test
  public void testColumnOfVaryingScale_PDI_19619_precision_3_single_number() {
    testNumber( "#.000", "1.999" ); // FAILS - returns mask "#.#"
  }

  // DEBUG
  //copy of test from Pentaho-kettle/integration/src/it/java/org/pentaho/di/core/util/StringEvaluatorIT.java#testLength_IfEvaluationResultIsNumber()
  @Test
  public void testLength_IfEvaluationResultIsNumber(){
    String[] numbers = new String[] { "1010.10101010", "10.01", "4,309.88" };
    for ( String value : numbers ) {
      evaluator.evaluateString( value );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    //assertEquals( "Number", result.getConversionMeta().getTypeDesc() ); // FIXME ignoring type for now
    assertEquals( 8, result.getConversionMeta().getPrecision() )
    assertEquals( 13, result.getConversionMeta().getLength() );
    assertEquals("#,###,###.#", result.getConversionMeta().getConversionMask());
    /** FIXME
     * matching existing implementation of masking, but should "1010.10101010" get a single decimal place mask?
     * StringEvaluator#DEFAULT_NUMBER_FORMATS highest mask is "#.000000", should we dynamically create a mask
     * that is 8 decimal places ie "#.00000000"
     *
     * original code back in March 6, 2010 https://github.com/pentaho/pentaho-kettle/blob/b5a786f6d9b4ef749c0d79d0bd835563619cc904/src-core/org/pentaho/di/core/util/StringEvaluator.java#L105
     * had masks such as:
     *    "#,###,###,###,###,###.############",
     *    "#.00000000000",
     *    "00.00000000000",
     *    "000000.00000000000",
     */
  }


  // DEBUG
  // Test to see how commas are handled with a precision greater than 6
  @Test
  public void testNumberWithCommasAndPrecision(){
    String[] numbers = new String[] { "1,234.9876543210" };
    for ( String value : numbers ) {
      evaluator.evaluateString( value );
    }
    StringEvaluationResult result = evaluator.getAdvicedResult();
    //assertEquals( "Number", result.getConversionMeta().getTypeDesc() );
    assertEquals( 10, result.getConversionMeta().getPrecision() );
    assertEquals( 16, result.getConversionMeta().getLength() );
    assertEquals("#,###,###.#", result.getConversionMeta().getConversionMask());
    // FIXME same issue above in #testLength_IfEvaluationResultIsNumber() with numbers with more than 6 decimal places
  }

  /////////////////////////////////////
  // currency types
  ////////////////////////////////////
  @Test
  public void testCurrency() {
    testCurrencyBasic( "+123", "-123", "(123)"  );
  }

  private void testCurrencyBasic( String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertTrue( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isString() );
  }

  /////////////////////////////////////
  // boolean types
  ////////////////////////////////////
  @Test
  public void testBooleanY() {
    testBoolean( "Y" );
  }

  @Test
  public void testBooleanN() {
    testBoolean( "N" );
  }

  @Test
  public void testBooleanTrue() {
    testBoolean( "True" );
  }

  @Test
  public void testBooleanFalse() {
    testBoolean( "False" );
  }

  private void testBoolean( String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isBoolean() );
  }

  /////////////////////////////////////
  // Date types, use default USA format
  ////////////////////////////////////
  @Test
  public void testDate() {
    testDefaultDateFormat( "MM/dd/yyyy", "10/10/2000" );
  }

  @Test
  public void testDateArray() {
    testDefaultDateFormat( "MM/dd/yyyy", "10/10/2000", "11/10/2000", "12/10/2000"  );
  }

  @Test
  public void testTimeStamp() {
    testDefaultDateFormat( "MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00" );
  }

  @Test
  public void testTimeStampSeconds() {
    testDefaultDateFormat( "MM/dd/yyyy HH:mm:ss", "10/10/2000 00:00:00" );
  }

  private void testDefaultDateFormat( String maskEn, String... strings ) {
    for ( String string : strings ) {
      evaluator.evaluateString( string );
    }
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    String actualMask = evaluator.getAdvicedResult().getConversionMeta().getConversionMask();
    assertTrue( maskEn.equals( actualMask ) );
  }

  @Test
  public void testDate2YearDigits() {
    testDefaultDateFormat( "MM/dd/yy", "10/10/20", "11/10/20", "12/10/20" );
  }

  @Test
  public void testCustomDateFormat() {
    String sampleFormat = "MM/dd/yyyy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<String>();
    dateFormats.add( sampleFormat );
    StringEvaluator evaluator = new StringEvaluator( true, new ArrayList<String>(), dateFormats );
    evaluator.evaluateString( "02/29/2000 00:00:00"  );
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    assertTrue( sampleFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

  @Test
  public void testAdviceedOneDateFormat() {
    String sampleLongFormat = "MM/dd/yyyy HH:mm:ss";
    String sampleShortFormat = "MM/dd/yy HH:mm:ss";
    ArrayList<String> dateFormats = new ArrayList<String>();
    dateFormats.add( sampleLongFormat );
    dateFormats.add( sampleShortFormat );
    StringEvaluator evaluator = new StringEvaluator( true, new ArrayList<String>(), dateFormats );
    evaluator.evaluateString( "02/29/20 00:00:00"  );
    assertFalse( evaluator.getStringEvaluationResults().isEmpty() );
    assertTrue( evaluator.getAdvicedResult().getConversionMeta().isDate() );
    assertFalse( sampleLongFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
    //should advice short format
    assertTrue( sampleShortFormat.equals( evaluator.getAdvicedResult().getConversionMeta().getConversionMask() ) );
  }

}
