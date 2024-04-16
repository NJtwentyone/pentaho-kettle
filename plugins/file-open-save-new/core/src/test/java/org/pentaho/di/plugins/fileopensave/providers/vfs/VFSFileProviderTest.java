/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.plugins.fileopensave.providers.vfs;

import junit.framework.TestCase;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.di.plugins.fileopensave.providers.vfs.model.VFSFile;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VFSFileProviderTest extends TestCase {

  public void testIsConnectionRoot() {

    VFSFileProvider testInstance = new VFSFileProvider();

    // TEST - simple negative tests before "pvfs://domain"
    assertFalse( testInstance.isConnectionRoot( null ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( null ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "" ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( " " ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs" ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs:" ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs:/" ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs://" ) ) );

    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs:///" ) ) );

    // TEST "pvfs://domain"
    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://someConnection" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://some_Connection" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://some-Connection" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://someConnection123" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://123someConnection123" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://someConnection/" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://some_Connection/" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://some-Connection/" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://someConnection123/" ) ) );

    assertTrue( testInstance.isConnectionRoot( createTestInstance( "pvfs://123someConnection123/" ) ) );

    // TEST now we have past the root domain
    assertFalse( testInstance.isConnectionRoot( createTestInstance( "pvfs://someConnection/someFolderA" ) ) );

    assertFalse( testInstance.isConnectionRoot(
        createTestInstance( "pvfs://someConnection/someFolderA/directory2" ) ) );

    assertFalse( testInstance.isConnectionRoot(
        createTestInstance( "pvfs://someConnection/someFolderA/directory2/randomFileC.txt" ) ) );
  }

  public void testHasBuckets() {

    VFSFileProvider testInstance = new VFSFileProvider();

    // TEST non VFSConnectionDetails
    ConnectionDetails mockConnectionDetails  = mock( ConnectionDetails.class );
    assertFalse( testInstance.hasBuckets( mockConnectionDetails ) );

    // TEST  does not have buckets
    VFSConnectionDetails mock_NoBuckets_VFSConnectionDetails  = mock( VFSConnectionDetails.class );
    when( mock_NoBuckets_VFSConnectionDetails.hasBuckets() ).thenReturn( false );
    assertFalse( testInstance.hasBuckets( mock_NoBuckets_VFSConnectionDetails ) );

    // TEST has buckets
    VFSConnectionDetails mock_Buckets_VFSConnectionDetails  = mock( VFSConnectionDetails.class );
    when( mock_Buckets_VFSConnectionDetails.hasBuckets() ).thenReturn( true );
    assertTrue( testInstance.hasBuckets( mock_Buckets_VFSConnectionDetails ) );
  }

  protected VFSFile createTestInstance( String path ) {
    VFSFile vfsFile = new VFSFile();
    vfsFile.setPath( path );
    return vfsFile;
  }


}
