/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2020-2024 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.ConnectionProvider;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.di.connections.vfs.VFSConnectionProvider;
import org.pentaho.di.connections.vfs.VFSRoot;
import org.pentaho.di.connections.vfs.provider.ConnectionFileProvider;
import org.pentaho.di.connections.vfs.provider.ConnectionFileSystem;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.plugins.fileopensave.api.overwrite.OverwriteStatus;
import org.pentaho.di.plugins.fileopensave.api.providers.BaseFileProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.Tree;
import org.pentaho.di.plugins.fileopensave.api.providers.Utils;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileException;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileNotFoundException;
import org.pentaho.di.plugins.fileopensave.providers.vfs.model.VFSDirectory;
import org.pentaho.di.plugins.fileopensave.providers.vfs.model.VFSFile;
import org.pentaho.di.plugins.fileopensave.providers.vfs.model.VFSLocation;
import org.pentaho.di.plugins.fileopensave.providers.vfs.model.VFSTree;
import org.pentaho.di.plugins.fileopensave.providers.vfs.service.KettleVFSService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;

/**
 * Created by bmorrise on 2/14/19.
 */
public class VFSFileProvider extends BaseFileProvider<VFSFile> {

  public static final String NAME = "VFS Connections";
  public static final String TYPE = "vfs";

  public static final String CONNECTION_SCHEME = ConnectionFileSystem.DOMAIN_ROOT;

  /**
   * Regular expression for connection name
   * // TODO this should be defined somewhere else like in package {@link VFSConnectionDetails}
   */
  protected static final String CONNECTION_NAME_REGEX = "[\\w_-]+";

  /**
   * Regular expression for optional folder slash at the end of a path
   */
  protected static final String OPTIONAL_FOLDER_SLASH_REGEX = "[/]?";

  /**
   * Regular expression for the expected pvfs path that only contains the connection name
   * ie "pvfs://someConnectionName".
   */
  protected static final String CONNECTION_NAME_ROOT_REGEX = CONNECTION_SCHEME
      + CONNECTION_NAME_REGEX + OPTIONAL_FOLDER_SLASH_REGEX;

  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;
  private Map<String, List<VFSFile>> roots = new HashMap<>();

  protected KettleVFSService kettleVFSService;

  public VFSFileProvider() {
    this( new KettleVFSService() );
  }

  public VFSFileProvider( KettleVFSService kettleVFSService ) {
    this.kettleVFSService = kettleVFSService;
  }

  @Override public Class<VFSFile> getFileClass() {
    return VFSFile.class;
  }

  @Override public String getName() {
    return NAME;
  }

  @Override public String getType() {
    return TYPE;
  }

  /**
   * Determines is a <code>filePath</code> is a VFS file
   * @param filePath
   * @return true if VFS file, false otherwise.
   */
  public boolean isSupported( String filePath ) {
    if ( filePath == null ) {
      return false;
    }
    boolean ret = false;
    try {
      URI uriFilePath = new URI( filePath );
      String testScheme = uriFilePath.getScheme() + "://";
      ret = uriFilePath.getScheme() != null && testScheme.matches( ConnectionFileSystem.DOMAIN_ROOT );
    } catch ( PatternSyntaxException | URISyntaxException e ) {
      // DO NOTHING
    }
    return ret;
  }

  /**
   * @return
   */
  public boolean isAvailable() {
    return true;
  }

  /**
   * Default implementation; typically not called.
   *
   * @return Unfiltered Tree
   */
  @Override public Tree getTree() {
    return getTree( new ArrayList<>() );
  }

  /**
   * Filtered implementation
   *
   * @return Filter Tree
   */
  @Override public VFSTree getTree( List<String> connectionTypes ) {
    VFSTree vfsTree = new VFSTree( NAME );

    List<ConnectionProvider<? extends ConnectionDetails>> providers =
      connectionManagerSupplier.get().getProvidersByType( VFSConnectionProvider.class );

    for ( ConnectionProvider<? extends ConnectionDetails> provider : providers ) {
      for ( ConnectionDetails connectionDetails : provider.getConnectionDetails() ) {
        VFSConnectionDetails vfsConnectionDetails = (VFSConnectionDetails) connectionDetails;
        VFSLocation vfsLocation = new VFSLocation();
        vfsLocation.setName( connectionDetails.getName() );
        vfsLocation.setRoot( NAME );
        vfsLocation.setHasChildren( true );
        vfsLocation.setCanDelete( false );
        String path = ConnectionFileProvider.SCHEME + "://" +  vfsConnectionDetails.getName(); // just set to pvfs://<connectionName>
        vfsLocation.setPath( path );
        vfsLocation.setDomain( vfsConnectionDetails.getDomain() );
        vfsLocation.setConnection( connectionDetails.getName() ); // TODO dont set once refactor calls to KettleVFS#getFileObject, VFSFile#create, and VFSDirectory#create
        vfsLocation.setCanAddChildren( vfsConnectionDetails.hasBuckets() );
        if ( connectionDetails.getType().startsWith( "s3" ) || connectionDetails.getType().startsWith( "snw" ) ) { // TODO re-evaluate use VFSConnectionDetail#hasBuckets
          vfsLocation.setHasBuckets( true );
        }
        if ( connectionTypes.isEmpty() || connectionTypes.contains( connectionDetails.getType() ) ) {
          vfsTree.addChild( vfsLocation );
        }
      }
    }

    return vfsTree;
  }

  /**
   * Retrieves the "bucket" folders for a connection. These folders are populated from a call
   * to {@link VFSConnectionProvider#getLocations(VFSConnectionDetails)}.
   * <p/>
   *
   * For "bucket" folders the complete list of functions for {@link FileObject} can't be called.
   * //TODO finish find out where/why Apache FileObjects fail on bucket locations.
   * <ul>
   *   <li>{@link FileObject#createFile()}</li>
   * </ul>
   *
   * @param fileDomainRoot
   * @return
   */
  private List<VFSFile> getRoot( VFSFile fileDomainRoot ) throws FileException {
    if ( this.roots.containsKey( getKey( fileDomainRoot ) ) ) {
      return this.roots.get( getKey( fileDomainRoot ) );
    }

    ConnectionDetails connectionDetails = ConnectionManager.getInstance()
        .getConnectionDetails( fileDomainRoot.getConnection() );

    if ( !hasBuckets( connectionDetails ) ) {
      return null;
    }

    VFSConnectionDetails vfsConnectionDetails = (VFSConnectionDetails) connectionDetails;

    @SuppressWarnings( "unchecked" )
    VFSConnectionProvider<VFSConnectionDetails> vfsConnectionProvider =
      (VFSConnectionProvider<VFSConnectionDetails>) ConnectionManager.getInstance()
        .getConnectionProvider( vfsConnectionDetails.getType() );

    List<VFSRoot> vfsRoots;
    try {
      vfsRoots = vfsConnectionProvider.getLocations( vfsConnectionDetails );
    } catch ( Exception e ) {
      throw new FileException( "Error getting VFS locations. Check your credentials and for connectivity." + e.getMessage(), e );
    }
    if ( vfsRoots == null || vfsRoots.isEmpty() ) {
      throw new FileNotFoundException( fileDomainRoot.getPath(), fileDomainRoot.getProvider() );
    }

    List<VFSFile> files = new ArrayList<>();

    String scheme = vfsConnectionProvider.getProtocol( vfsConnectionDetails );
    for ( VFSRoot root : vfsRoots ) {
      VFSDirectory vfsDirectory = new VFSDirectory();
      vfsDirectory.setName( root.getName() );
      vfsDirectory.setDate( root.getModifiedDate() );
      vfsDirectory.setHasChildren( true );
      vfsDirectory.setCanAddChildren( true );
      vfsDirectory.setParent( scheme + "://" ); // TODO investigate how is this used?
      vfsDirectory.setDomain( vfsConnectionDetails.getDomain() ); // TODO is this needed and does it make sense
      vfsDirectory.setConnection( vfsConnectionDetails.getName() );  // TODO dont set once refactor calls to KettleVFS#getFileObject, VFSFile#create, and VFSDirectory#create
      vfsDirectory.setPath( fileDomainRoot.getPath() + '/' + root.getName() ); // TODO use deliminator from another class or path.
      vfsDirectory.setRoot( NAME );
      files.add( vfsDirectory );
    }
    this.roots.put( getKey( fileDomainRoot ), files );
    return files;
  }

  /**
   * Get key for <code>file</code>
   * @param vfsFile
   * @return
   */
  protected String getKey( VFSFile vfsFile ) {
    return vfsFile.getPath();
  }

  /**
   * Determines if a <code>connectionDetails</code> has "buckets".
   * @param connectionDetails
   * @return
   */
  protected boolean hasBuckets( ConnectionDetails connectionDetails ) {
    return connectionDetails instanceof VFSConnectionDetails && ( (VFSConnectionDetails) connectionDetails).hasBuckets();
  }

  /**
   * @param file
   * @param filters
   * @return
   */
  @Override
  public List<VFSFile> getFiles( VFSFile file, String filters, VariableSpace space ) throws FileException {
    if (  isConnectionRoot( file ) ) {
      List<VFSFile> rootFiles = getRoot( file );
      if ( rootFiles != null ) {
        return rootFiles;
      }
    }
    FileObject fileObject;
    try {
      fileObject = getFileObject( file, space );
    } catch ( FileException e ) {
      throw new FileNotFoundException( file.getPath(), TYPE );
    }
    return populateChildren( file, fileObject, filters );
  }

  /**
   * Determines if <code>file</code>'s path is the URI domain root.
   * @param file
   * @return true if <code>file</code>'s path is only at domain, false otherwise.
   */
  protected boolean isConnectionRoot( VFSFile file ) {
    return file != null && !StringUtil.isEmpty( file.getPath() )
        && file.getPath().matches( CONNECTION_NAME_ROOT_REGEX );
  }

  /**
   * Check if a file object has children
   *
   * @param fileObject
   * @return
   */
  private boolean hasChildren( FileObject fileObject ) {
    try {
      return fileObject != null && fileObject.getType().hasChildren();
    } catch ( FileSystemException e ) {
      return false;
    }
  }

  /**
   * Get the children if they are available, if an error return an empty list
   *
   * @param fileObject
   * @return
   */
  private FileObject[] getChildren( FileObject fileObject ) {
    try {
      return fileObject != null ? fileObject.getChildren() : new FileObject[] {};
    } catch ( FileSystemException e ) {
      return new FileObject[] {};
    }
  }

  /**
   * Populate VFS file objects from vfs FileObject types
   *
   * @param parent
   * @param fileObject
   * @param filters
   * @return
   */
  private List<VFSFile> populateChildren( VFSFile parent, FileObject fileObject, String filters ) {
    List<VFSFile> files = new ArrayList<>();
    if ( fileObject != null && hasChildren( fileObject ) ) {
      FileObject[] children = getChildren( fileObject );
      for ( FileObject child : children ) {
        if ( hasChildren( child ) ) {
          files.add( VFSDirectory.create( parent.getPath(), child, parent.getConnection(), parent.getDomain() ) );
        } else {
          if ( child != null && Utils.matches( child.getName().getBaseName(), filters ) ) {
            files.add( VFSFile.create( parent.getPath(), child, parent.getConnection(), parent.getDomain() ) );
          }
        }
      }
    }
    return files;
  }

  @Override public VFSFile getFile( VFSFile file, VariableSpace space ) {
    try {
      FileObject fileObject = getFileObject( file, space );
      if ( !fileObject.exists() ) { // NOTE: parent call to FileController#getFile(File) is not used
        return null;
      }
      String parent = null;
      if ( fileObject.getParent() != null && fileObject.getParent().getName() != null ) {
        parent = fileObject.getParent().getName().getURI();
      } else {
        parent = fileObject.getURL().getProtocol() + "://";
      }
      if ( fileObject.getType().equals( FileType.FOLDER ) ) {
        return VFSDirectory.create( parent, fileObject, null, file.getDomain() );
      } else {
        return VFSFile.create( parent, fileObject, null, file.getDomain() );
      }
    } catch ( FileException | FileSystemException e ) {
      // File does not exist
    }
    return null;
  }

  /**
   * @param files
   * @param space
   * @return
   */
  @Override
  public List<VFSFile> delete( List<VFSFile> files, VariableSpace space ) {
    List<VFSFile> deletedFiles = new ArrayList<>();
    for ( VFSFile file : files ) {
      try {
        FileObject fileObject = getFileObject( file, space );
        if ( fileObject.delete( getAllFileSelector() ) > 0 ) {
          deletedFiles.add( file );
        }
      } catch ( FileException | FileSystemException kfe ) {
        // Ignore don't add
      }
    }
    return deletedFiles;
  }

  /**
   * @param folder
   * @return
   */
  @Override public VFSFile add( VFSFile folder, VariableSpace space ) {
    try { // NOTE: parent call from FileController#add(File) is not used for VFSFileProvider
      FileObject fileObject = getFileObject( folder.getPath(), space );
      fileObject.createFolder();
      String parent = folder.getPath().substring( 0, folder.getPath().length() - 1 );
      return VFSDirectory.create( parent, fileObject, folder.getConnection(), folder.getDomain() );
    } catch ( FileException | FileSystemException ignored ) {
      // Ignored
    }
    return null;
  }

  /**
   * @param file
   * @param newPath
   * @param overwriteStatus
   * @param space
   * @return
   */
  @Override public VFSFile rename( VFSFile file, String newPath, OverwriteStatus overwriteStatus, VariableSpace space ) {
    return doMove( file, newPath, overwriteStatus, space );
  }

  /**
   * @param file
   * @param toPath
   * @param overwriteStatus
   * @Parem space
   * @return
   */
  @Override
  public VFSFile move( VFSFile file, String toPath, OverwriteStatus overwriteStatus, VariableSpace space ) {
    return doMove( file, toPath, overwriteStatus, space );
  }

  /**
   * @param file
   * @param newPath
   * @param overwriteStatus
   * @return
   */
  private VFSFile doMove( VFSFile file, String newPath, OverwriteStatus overwriteStatus, VariableSpace space ) {
    try {
      FileObject fileObject = getFileObject( file.getPath(), space );
      FileObject renameObject = getFileObject( newPath, space );

      if ( renameObject.exists() ) {
        overwriteStatus.promptOverwriteIfNecessary( file.getPath(),
          file.getEntityType().isDirectory() ? "folder" : "file" );
        if ( overwriteStatus.isOverwrite() ) {
          renameObject.delete();
        } else if ( overwriteStatus.isCancel() || overwriteStatus.isSkip() ) {
          return null;
        } else if ( overwriteStatus.isRename() ) {
          VFSDirectory vfsDir =
            VFSDirectory.create( renameObject.getParent().getPath().toString(), renameObject, file.getConnection(),
              file.getDomain() );
          newPath = getNewName( vfsDir, newPath, space  );
          renameObject = getFileObject( newPath, space );
        }
      }
      fileObject.moveTo( renameObject );
      if ( file instanceof VFSDirectory ) {
        return VFSDirectory.create( renameObject.getParent().getPublicURIString(), renameObject, file.getConnection(),
          file.getDomain() );
      } else {
        return VFSFile.create( renameObject.getParent().getPublicURIString(), renameObject, file.getConnection(),
          file.getDomain() );
      }
    } catch ( FileException | FileSystemException e ) {
      return null;
    }
  }

  /**
   * Note that this copy will only copy files within the SAME VFS connection.  For copies across different
   * connections use {@link org.pentaho.di.plugins.fileopensave.controllers.FileController#copyFileBetweenProviders}
   * @param file
   * @param toPath
   * @param overwriteStatus
   * @return
   * @throws FileException
   */
  @Override
  public VFSFile copy( VFSFile file, String toPath, OverwriteStatus overwriteStatus, VariableSpace space ) throws FileException {
    try {
      overwriteStatus.setCurrentFileInProgressDialog( file.getPath() );

      FileObject fileObject = getFileObject( file, space );
      FileObject copyObject = getFileObject( toPath, space );
      overwriteStatus.promptOverwriteIfNecessary( copyObject.exists(), toPath,
        file.getEntityType().isDirectory() ? "folder" : "file"
        , null,
        "Note: Once this decision is made, the entire folder will be copied using a faster copy within the same"
          + " connection.  However, Any duplicate file encountered along the way can only be overwritten or skipped. "
          + " It can not be renamed." ); // TODO move to i18n messages
      if ( overwriteStatus.isCancel() || overwriteStatus.isSkip() ) {
        return null;
      }

      VFSFile toDirectory = null;
      if ( overwriteStatus.isRename() ) {
        toDirectory = VFSDirectory.create( copyObject.getParent().getPublicURIString(), copyObject,
          file.getConnection(), file.getDomain() );
        String newDestination = getNewName( toDirectory, copyObject.getName().toString(), space );
        copyObject = getFileObject( newDestination, space );
      }

      copyObject.copyFrom( fileObject, new OverwriteAwareFileSelector( overwriteStatus, fileObject, copyObject,
        file.getConnection(), space ) );
      // Now get the return value
      if ( file instanceof VFSDirectory ) {
        return VFSDirectory
          .create( copyObject.getParent().getPublicURIString(), fileObject, file.getConnection(), file.getDomain() );
      } else {
        return VFSFile
          .create( copyObject.getParent().getPublicURIString(), fileObject, file.getConnection(), file.getDomain() );
      }
    } catch ( FileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  /**
   * @param dir
   * @param path
   * @return
   * @throws FileException
   */
  @Override public boolean fileExists( VFSFile dir, String path, VariableSpace space ) throws FileException {
    String sanitizeName = sanitizeName( dir, path ); // FIXME possible existing bug for copy + paste scenario, only works in overwrite scenario of <code>path<code/>, should check <code>dir</code> exists only
    try {
      FileObject fileObject = getFileObject( sanitizeName, space );
      return fileObject.exists();
    } catch ( FileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  /**
   * @param file
   * @param space
   * @return
   */
  @Override
  public InputStream readFile( VFSFile file, VariableSpace space ) {
    try {
      FileObject fileObject = getFileObject( file.getPath(), space );
      return fileObject.getContent().getInputStream();
    } catch ( FileException | FileSystemException e ) {
      return null;
    }
  }

  /**
   * @param inputStream
   * @param destDir
   * @param path
   * @param overwriteStatus
   * @return
   * @throws FileException
   */
  @Override
  public VFSFile writeFile( InputStream inputStream, VFSFile destDir, String path, OverwriteStatus overwriteStatus,
                            VariableSpace space ) throws FileException {
    FileObject fileObject = getFileObject( path, space );
    if ( fileObject != null ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream(); ) {
        IOUtils.copy( inputStream, outputStream );
        outputStream.flush();
        return VFSFile.create( destDir.getPath(), fileObject, destDir.getConnection(), destDir.getDomain() );
      } catch ( IOException e ) {
        return null;
      }
    }
    return null;
  }

  /**
   * @param file1
   * @param file2
   * @return
   */
  @Override
  public boolean isSame( org.pentaho.di.plugins.fileopensave.api.providers.File file1,
                         org.pentaho.di.plugins.fileopensave.api.providers.File file2 ) {
    if ( file1 instanceof VFSFile && file2 instanceof VFSFile ) {
      VFSFile vfsFile1 = (VFSFile) file1;
      VFSFile vfsFile2 = (VFSFile) file2;
      return vfsFile1.getConnection().equals( vfsFile2.getConnection() );
    }
    return false;
  }

  /**
   * @param destDir
   * @param newPath
   * @return
   * @throws FileException
   */
  @Override public String getNewName( VFSFile destDir, String newPath, VariableSpace space ) throws FileException {
    String extension = Utils.getExtension( newPath );
    String parent = Utils.getParent( newPath, "/" );
    String name = Utils.getName( newPath, "/" ).replace( "." + extension, "" );
    int i = 1;
    String testName = sanitizeName( destDir, newPath );
    try {
      while ( getFileObject( testName,  space ).exists() ) {// TODO possible bug, what is destDir is bad path ?
        if ( Utils.isValidExtension( extension ) ) {
          testName = sanitizeName( destDir, parent + name + "_" + i + "." + extension );
        } else {
          testName = sanitizeName( destDir, newPath + "_" + i );
        }
        i++;
      }
    } catch ( FileException | FileSystemException e ) { // TODO how is the function determining between bad directory and found testName is an available name?
      return testName;
    }
    return testName;
  }

  /**
   * @param file
   * @return
   */
  @Override public VFSFile getParent( VFSFile file ) {
    VFSFile vfsFile = new VFSFile();
    vfsFile.setConnection( file.getConnection() );
    vfsFile.setPath( file.getParent() );
    return vfsFile;
  }

  @Override public String sanitizeName( VFSFile destDir, String newPath ) {
    if ( newPath.startsWith( ConnectionFileProvider.SCHEME + "://" ) ) {
      return newPath;
    }
      return getConnectionProvider( newPath ).sanitizeName( newPath ); // FIXME might be obsolete
  }

  private VFSConnectionProvider<VFSConnectionDetails> getConnectionProvider( String key ) { // FIXME might be obsolete
    @SuppressWarnings( "unchecked" )
    VFSConnectionProvider<VFSConnectionDetails> vfsConnectionProvider =
      (VFSConnectionProvider<VFSConnectionDetails>) ConnectionManager.getInstance()
        .getConnectionProvider( key );
    return vfsConnectionProvider;
  }

  public void clearProviderCache() {
    this.roots = new HashMap<>();
  }

  @Override public VFSFile createDirectory( String parentPath, VFSFile fileParent, String newDirectoryName ) {
    try {
      String newDirectoryPath = fileParent.getPath() + VFSFile.DELIMITER + newDirectoryName; // TODO avoid URI manipulation
      FileObject fileObject = getFileObject( newDirectoryPath, new Variables() );
      fileObject.createFolder();
      return VFSDirectory.create( parentPath, fileObject, fileParent.getConnection(), fileParent.getDomain() ); // TODO "should" refactor to call VFSFileProvider#add( VFSFile folder, VariableSpace space ) similar as other classes in org.pentaho.di.plugins.fileopensave, could just call #add(..) with dummy VSFFile.getpath=newDirectoryPath
    } catch ( FileException | FileSystemException ignored ) {
      // Ignored
    }
    return null;
  }

  /**
   * Wrapper around {@link KettleVFSService#getFileObject(String, VariableSpace)}
   * @param file
   * @param space
   * @return
   * @throws FileException
   */
  protected FileObject getFileObject( VFSFile file, VariableSpace space )  throws FileException {
    return getFileObject( file.getPath(), space );
  }

  /**
   * Wrapper around {@link KettleVFSService#getFileObject(String, VariableSpace)}
   * @param vfsPath
   * @param space
   * @return
   * @throws FileException
   */
  protected FileObject getFileObject( String vfsPath, VariableSpace space )  throws FileException {
    return kettleVFSService.getFileObject( vfsPath, space ); // TODO maybe just need Supplier instead of whole new class
  }


  private FileSelector getAllFileSelector() {
    return new FileSelector(){
      @Override public boolean includeFile( FileSelectInfo fileInfo ) throws Exception {
        return true;
      }
      @Override public boolean traverseDescendents( FileSelectInfo fileInfo ) throws Exception {
        return true;
      }
    };
  }

}
