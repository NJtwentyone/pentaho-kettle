/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2024 Hitachi Vantara. All rights reserved.
 */

package org.pentaho.di.connections.utils;

import junit.framework.TestCase;
import org.junit.Test;

public class ConnectionUriParserTest extends TestCase {

  /**
   * Full set of special characters the connection name can be.
   * Only excluding the character '/' which is a file path deliminator.
   */
  public static final String SPECIAL_CHARACTERS_FULL_TEST_SET = "!\"#$%&\'()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

  @Test
  public void testConnectionUriParser_Negative_Example_URIs() throws Exception {

    assertNullValues( new ConnectionUriParser( null ) );

    assertNullValues( new ConnectionUriParser( "" ) );

    assertNullValues( new ConnectionUriParser( "      " ) );

    assertNullValues( new ConnectionUriParser( "someGarbage" ) );

  }

  @Test
  public void testConnectionUriParser_Negative_Example_Non_URIs() throws Exception {

    assertNullValues( new ConnectionUriParser( "/someUser/someUnixFile" ) );

    assertNullValues( new ConnectionUriParser(  "T:\\Users\\RandomSUser\\Documents\\someWindowsFile" ) );

    assertNullValues( new ConnectionUriParser(  "//home/randomUser/randomFile.rpt" ) ); // Pentaho repository

  }

  @Test
  public void testConnectionUriParser_Example_URIs() throws Exception {

    String uri_01 = "xyz://";
    assertEquals( "xyz", new ConnectionUriParser( uri_01 ) );

    String uri_02 = "xyz:///";
    assertEquals( "xyz", new ConnectionUriParser( uri_02 ) );

    String uri_03 = "pvfs://";
    assertEquals( "pvfs", new ConnectionUriParser( uri_03 ));

    String uri_04 = "pvfs:///";
    assertEquals( "pvfs", new ConnectionUriParser( uri_04 ) );

    String uri_05 = "xyz://abc";
    assertEquals( "xyz", "abc", new ConnectionUriParser( uri_05 ) );

    String uri_06 = "xyz://abc/";
    assertEquals( "xyz", "abc", new ConnectionUriParser( uri_06 ) );

    String uri_07 = "xyz://abc/someDir/somePath/someFile.txt";
    assertEquals( "xyz", "abc", "/someDir/somePath/someFile.txt", new ConnectionUriParser( uri_07 ) );

    String uri_08 = "pvfs://some-ConnectionName_123/";
    assertEquals( "pvfs", "some-ConnectionName_123", new ConnectionUriParser(  uri_08 ) );

    String uri_09 = "pvfs://some-ConnectionName_123";
    assertEquals( "pvfs", "some-ConnectionName_123", new ConnectionUriParser(  uri_09 ) );

    String uri_10 = "pvfs://some-ConnectionName_123/someFolderA/someFolderB/someFolderC/sales_data.csv";
    assertEquals( "pvfs",
        "some-ConnectionName_123",
        "/someFolderA/someFolderB/someFolderC/sales_data.csv",
        new ConnectionUriParser(  uri_10 ) );

    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri_11 = "pvfs://Special Character name &#! <> why would you do this/someFolderA/someFolderB/someFolderC/sales_data.csv";
    assertEquals( "pvfs",
      "Special Character name &#! <> why would you do this",
      "/someFolderA/someFolderB/someFolderC/sales_data.csv",
      new ConnectionUriParser(  uri_11 ) );


    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri_12 = "pvfs://Special Character name &#! <> why would you do this";
    assertEquals( "pvfs",
      "Special Character name &#! <> why would you do this",
      new ConnectionUriParser(  uri_12 ) );

    String uri_13 = "pvfs://Special Character name &#! <> why would you do this/";
    assertEquals( "pvfs",
      "Special Character name &#! <> why would you do this",
      new ConnectionUriParser(  uri_13 ) );

    // TEST robust example of special characters
    String uri_14 = "pvfs://" + SPECIAL_CHARACTERS_FULL_TEST_SET;
    assertEquals( "pvfs",
      SPECIAL_CHARACTERS_FULL_TEST_SET,
      new ConnectionUriParser(  uri_14 ) );

    String uri_15 = "pvfs://" + SPECIAL_CHARACTERS_FULL_TEST_SET + "/";
    assertEquals( "pvfs",
      SPECIAL_CHARACTERS_FULL_TEST_SET,
      new ConnectionUriParser(  uri_15 ) );

    String absolutePVFSPath = "/someFolderA/someFolderB/someFolderC/sales_data.csv";

    String uri_16 = "pvfs://" + SPECIAL_CHARACTERS_FULL_TEST_SET + absolutePVFSPath;
    assertEquals( "pvfs",
      SPECIAL_CHARACTERS_FULL_TEST_SET,
      absolutePVFSPath,
      new ConnectionUriParser(  uri_16 ) );

  }

  protected void assertEquals( String expectedScheme, String expectedConnectionName, String expectedPvfsPath,
                              ConnectionUriParser actualConnectionUriParser ) {
    assertEquals( expectedScheme, actualConnectionUriParser.getScheme() );
    assertEquals( expectedConnectionName, actualConnectionUriParser.getConnectionName() );
    assertEquals( expectedPvfsPath, actualConnectionUriParser.getPvfsPath() );
  }

  protected void assertEquals( String expectedScheme, String expectedConnectionName,
                               ConnectionUriParser actualConnectionUriParser ) {
    assertEquals( expectedScheme, actualConnectionUriParser.getScheme() );
    assertEquals( expectedConnectionName, actualConnectionUriParser.getConnectionName() );
    assertEquals( null, actualConnectionUriParser.getPvfsPath() );
  }

  protected void assertEquals( String expectedScheme,
                               ConnectionUriParser actualConnectionUriParser ) {
    assertEquals( expectedScheme, actualConnectionUriParser.getScheme() );
    assertEquals( null, actualConnectionUriParser.getConnectionName() );
    assertEquals( null, actualConnectionUriParser.getPvfsPath() );
  }

  protected void assertNullValues( ConnectionUriParser actualConnectionUriParser ) {
    assertEquals( null, null, null, actualConnectionUriParser );
  }

}
