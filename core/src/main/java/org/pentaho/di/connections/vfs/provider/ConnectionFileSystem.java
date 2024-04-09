/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.connections.vfs.provider;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.BaseVFSConnectionDetails;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.di.connections.vfs.tranform.PocDefaultVFSUriTransformer;
import org.pentaho.di.connections.vfs.tranform.PocVFSUriTransformer;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.vfs.configuration.IKettleFileSystemConfigBuilder;
import org.pentaho.di.core.vfs.configuration.KettleFileSystemConfigBuilderFactory;

import java.util.Collection;
import java.util.function.Supplier;

public class ConnectionFileSystem extends AbstractFileSystem implements FileSystem {

  public static final String CONNECTION = "connection";
  public static final String DOMAIN_ROOT = "[\\w]+://";
  private Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;

  public ConnectionFileSystem( FileName rootName, FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
  }

  /**
   * Creates a url for {@link ConnectionFileName}
   *
   * @param abstractFileName  File name
   * @param connectionDetails Connection details for the file name
   * @return created url otherwise null
   */
  public static String getUrl( AbstractFileName abstractFileName, ConnectionDetails connectionDetails ) { // FIXME investigate why is this public and static !!?!?
    VFSConnectionDetails vfsConnectionDetails = (VFSConnectionDetails) connectionDetails; // FIXME investigate aren't we almost guaranteed to be of class VFSConnectionDetails
    return getVFSUriTransformer( vfsConnectionDetails ).toProviderUri( abstractFileName ); // handle all providers and basic url conversion scenario
  }

  @Override
  protected FileObject createFile( AbstractFileName abstractFileName ) throws Exception {

    String connectionName = ( (ConnectionFileName) abstractFileName ).getConnection();
    VFSConnectionDetails connectionDetails =
      (VFSConnectionDetails) connectionManager.get().getConnectionDetails( connectionName );
    FileSystemOptions opts = super.getFileSystemOptions();
    IKettleFileSystemConfigBuilder configBuilder = KettleFileSystemConfigBuilderFactory.getConfigBuilder
      ( new Variables(), ConnectionFileProvider.SCHEME );
    VariableSpace varSpace = (VariableSpace) configBuilder.getVariableSpace( super.getFileSystemOptions() );
    if ( connectionDetails != null ) {
      connectionDetails.setSpace( varSpace );
    }
    String url = getUrl( abstractFileName, connectionDetails );

    AbstractFileObject fileObject = null;
    String domain = null;

    if ( url != null ) {
      domain = connectionDetails.getDomain();
      varSpace.setVariable( CONNECTION, connectionName );
      fileObject = (AbstractFileObject) KettleVFS.getFileObject( url, varSpace );
    }

    return createConnectionFile( connectionDetails, abstractFileName, this, fileObject, domain );
  }

  /**
   * POC to support legacy VFSConnectionDetails
   * hack "abstract factory" creational design pattern
   * @param vfsConnectionDetails
   * @param abstractFileName
   * @param connectionFileSystem
   * @param fileObject
   * @param domain
   * @return
   */
  protected ConnectionFileObject createConnectionFile( VFSConnectionDetails vfsConnectionDetails,
        AbstractFileName abstractFileName, ConnectionFileSystem connectionFileSystem, AbstractFileObject fileObject,
          String domain ) {
    return (vfsConnectionDetails instanceof BaseVFSConnectionDetails )
        ? new ConnectionFileObject( abstractFileName, connectionFileSystem, fileObject, domain, ( (BaseVFSConnectionDetails) vfsConnectionDetails ) )
        : new ConnectionFileObject( abstractFileName, connectionFileSystem, fileObject, domain ); // NOTE: handles backwards compatibility
  }

  /**
   * POC to support legacy VFSConnectionDetails
   * simple "factory" creational design pattern
   * can be more complicated based on PVFS  & VFSConnectionDetails requirements
   * TODO investigate 'static' method signature forced by {@link #getUrl(AbstractFileName, ConnectionDetails)}
   * @param vfsConnectionDetails
   * @return
   */
  protected static PocVFSUriTransformer getVFSUriTransformer(VFSConnectionDetails vfsConnectionDetails ) {
    return (vfsConnectionDetails instanceof BaseVFSConnectionDetails )
        ? ( (BaseVFSConnectionDetails) vfsConnectionDetails )
        : new PocDefaultVFSUriTransformer( vfsConnectionDetails ); // NOTE: handle backwards compatibility
  }

  @Override protected void addCapabilities( Collection<Capability> collection ) {
    collection.addAll( ConnectionFileProvider.capabilities );
  }

  @Override
  public FileObject resolveFile( FileName name ) throws FileSystemException {
    try {
      return this.createFile( (AbstractFileName) name );
    } catch ( Exception e ) {
      throw new FileSystemException( "vfs.provider/resolve-file.error", name, e );
    }
  }

}
