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

  @Test
  public void testConnectionUriParser_Negative_Example_URIs() throws Exception {

    String uri_bad_01 = null;

    ConnectionUriParser cup_bad_01 = new ConnectionUriParser( uri_bad_01 );
    assertEquals( null, cup_bad_01.getScheme() );
    assertEquals( null, cup_bad_01.getConnectionName() );

    String uri_bad_02 = "";

    ConnectionUriParser cup_bad_02 = new ConnectionUriParser( uri_bad_02 );
    assertEquals( null, cup_bad_02.getScheme() );
    assertEquals( null, cup_bad_02.getConnectionName() );


    String uri_bad_03 = "      ";

    ConnectionUriParser cup_bad_03 = new ConnectionUriParser( uri_bad_03 );
    assertEquals( null, cup_bad_03.getScheme() );
    assertEquals( null, cup_bad_03.getConnectionName() );

    String uri_bad_04 = "someGarbage";

    ConnectionUriParser cup_bad_04 = new ConnectionUriParser( uri_bad_04 );
    assertEquals( null, cup_bad_04.getScheme() );
    assertEquals( null, cup_bad_04.getConnectionName() );

  }

  @Test
  public void testConnectionUriParser_Negative_Example_Non_URIs() throws Exception {

    String uri_bad_05 =  "/someUser/someUnixFile";

    ConnectionUriParser cup_bad_05 = new ConnectionUriParser( uri_bad_05 );
    assertEquals( null, cup_bad_05.getScheme() );
    assertEquals( null, cup_bad_05.getConnectionName() );

    String uri_bad_06 =  "T:\\Users\\RandomSUser\\Documents\\someWindowsFile";

    ConnectionUriParser cup_bad_06 = new ConnectionUriParser( uri_bad_06 );
    assertEquals( null, cup_bad_06.getScheme() );
    assertEquals( null, cup_bad_06.getConnectionName() );

    String uri_bad_07 =  "//home/randomUser/randomFile.rpt"; // Pentaho repository

    ConnectionUriParser cup_bad_07 = new ConnectionUriParser( uri_bad_07 );
    assertEquals( null, cup_bad_07.getScheme() );
    assertEquals( null, cup_bad_07.getConnectionName() );
  }

  @Test
  public void testConnectionUriParser_Example_URIs() throws Exception {

    String uri_01 = "xyz://";

    ConnectionUriParser cup_01 = new ConnectionUriParser( uri_01 );
    assertEquals( "xyz", cup_01.getScheme() );
    assertEquals( null, cup_01.getConnectionName() );

    String uri_02 = "xyz:///";

    ConnectionUriParser cup_02 = new ConnectionUriParser( uri_02 );
    assertEquals( "xyz", cup_02.getScheme() );
    assertEquals( null, cup_02.getConnectionName() );

    String uri_3 = "pvfs://";

    ConnectionUriParser cup_3 = new ConnectionUriParser( uri_3 );
    assertEquals( "pvfs", cup_3.getScheme() );
    assertEquals( null, cup_3.getConnectionName() );

    String uri_4 = "pvfs:///";

    ConnectionUriParser cup_4 = new ConnectionUriParser( uri_4 );
    assertEquals( "pvfs", cup_4.getScheme() );
    assertEquals( null, cup_4.getConnectionName() );

    String uri5 = "xyz://abc";

    ConnectionUriParser cup5 = new ConnectionUriParser( uri5 );
    assertEquals( "xyz", cup5.getScheme() );
    assertEquals( "abc", cup5.getConnectionName() );

    String uri6 = "xyz://abc/";

    ConnectionUriParser cup6 = new ConnectionUriParser( uri6 );
    assertEquals( "xyz", cup6.getScheme() );
    assertEquals( "abc", cup6.getConnectionName() );

    String uri7 = "xyz://abc/someDir/somePath/someFile.txt";

    ConnectionUriParser cup7 = new ConnectionUriParser( uri7 );
    assertEquals( "xyz", cup7.getScheme() );
    assertEquals( "abc", cup7.getConnectionName() );

    String uri8 = "pvfs://some-ConnectionName_123/";

    ConnectionUriParser cup8 = new ConnectionUriParser( uri8 );
    assertEquals( "pvfs", cup8.getScheme() );
    assertEquals( "some-ConnectionName_123", cup8.getConnectionName() );

    String uri9 = "pvfs://some-ConnectionName_123";

    ConnectionUriParser cup9 = new ConnectionUriParser( uri9 );
    assertEquals( "pvfs", cup9.getScheme() );
    assertEquals( "some-ConnectionName_123", cup9.getConnectionName() );

    String uri10 = "pvfs://some-ConnectionName_123/someFolderA/someFolderB/someFolderC/sales_data.csv";

    ConnectionUriParser cup10 = new ConnectionUriParser( uri10 );
    assertEquals( "pvfs", cup10.getScheme() );
    assertEquals( "some-ConnectionName_123", cup10.getConnectionName() );

    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri11 = "pvfs://Special Character name &#! <> why would you do this/someFolderA/someFolderB/someFolderC/sales_data.csv";

    ConnectionUriParser cup11 = new ConnectionUriParser( uri11 );
    assertEquals( "pvfs", cup11.getScheme() );
    assertEquals( "Special Character name &#! <> why would you do this", cup11.getConnectionName() );


    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri12 = "pvfs://Special Character name &#! <> why would you do this";

    ConnectionUriParser cup12 = new ConnectionUriParser( uri12 );
    assertEquals( "pvfs", cup12.getScheme() );
    assertEquals( "Special Character name &#! <> why would you do this", cup12.getConnectionName() );

    // TEST robust example of special characters, only excluding '/'
    String specialCharacters = "!\"#$%&\'()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
    String uri13 = "pvfs://" + specialCharacters;

    ConnectionUriParser cup13 = new ConnectionUriParser( uri13 );
    assertEquals( "pvfs", cup13.getScheme() );
    assertEquals( specialCharacters, cup13.getConnectionName() );

  }
}
