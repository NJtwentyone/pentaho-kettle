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

public class ConnectionUriParserTest extends TestCase {

  public void testConnectionUriParser_Example_URIs() throws Exception {

    String uri0 = "xyz://abc";

    ConnectionUriParser cup0 = new ConnectionUriParser( uri0 );
    assertEquals( "xyz", cup0.getScheme() );
    assertEquals( "abc", cup0.getConnectionName() );

    String uri1 = "xyz://abc/";

    ConnectionUriParser cup1 = new ConnectionUriParser( uri1 );
    assertEquals( "xyz", cup1.getScheme() );
    assertEquals( "abc", cup1.getConnectionName() );

    String uri2 = "xyz://abc/someDir/somePath/someFile.txt";

    ConnectionUriParser cup2 = new ConnectionUriParser( uri2 );
    assertEquals( "xyz", cup2.getScheme() );
    assertEquals( "abc", cup2.getConnectionName() );

    String uri3 = "pvfs://some-ConnectionName_123/";

    ConnectionUriParser cup3 = new ConnectionUriParser( uri3 );
    assertEquals( "pvfs", cup3.getScheme() );
    assertEquals( "some-ConnectionName_123", cup3.getConnectionName() );

    String uri4 = "pvfs://some-ConnectionName_123";

    ConnectionUriParser cup4 = new ConnectionUriParser( uri4 );
    assertEquals( "pvfs", cup4.getScheme() );
    assertEquals( "some-ConnectionName_123", cup4.getConnectionName() );

    String uri5 = "pvfs://some-ConnectionName_123/someFolderA/someFolderB/someFolderC/sales_data.csv";

    ConnectionUriParser cup5 = new ConnectionUriParser( uri5 );
    assertEquals( "pvfs", cup5.getScheme() );
    assertEquals( "some-ConnectionName_123", cup5.getConnectionName() );

    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri6 = "pvfs://Special Character name &#! <> why would you do this/someFolderA/someFolderB/someFolderC/sales_data.csv";

    ConnectionUriParser cup6 = new ConnectionUriParser( uri6 );
    assertEquals( "pvfs", cup6.getScheme() );
    assertEquals( "Special Character name &#! <> why would you do this", cup6.getConnectionName() );


    // TEST : can handle special characters, passing connection name as-is per "current" requirements based on connection creation logic
    String uri7 = "pvfs://Special Character name &#! <> why would you do this";

    ConnectionUriParser cup7 = new ConnectionUriParser( uri7 );
    assertEquals( "pvfs", cup7.getScheme() );
    assertEquals( "Special Character name &#! <> why would you do this", cup7.getConnectionName() );

  }
}
