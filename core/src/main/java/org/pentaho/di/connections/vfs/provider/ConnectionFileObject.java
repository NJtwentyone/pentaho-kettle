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

package org.pentaho.di.connections.vfs.provider;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.operations.FileOperations;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.pentaho.di.connections.vfs.tranform.PocLegacyGetChildVFSUriTransformer;
import org.pentaho.di.connections.vfs.tranform.PocVFSUriTransformer;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * The FileObject implementation for the PVFS (Pentaho VFS) vfs provider.
 *
 * The purpose of ConnectionFileObject is to represent a named connection FileObject. ConnectionFileObject holds a
 * reference to the resolved FileObject and delegates metadata calls to that reference object, but for other calls, such
 * as getChildren, ConnectionFileObject resolves the child FileObjects, but converts them to pvfs FileObjects to
 * maintain named connection information.
 */
public class ConnectionFileObject extends AbstractFileObject<ConnectionFileSystem> implements AliasedFileObject {

  public static final String DELIMITER = "/"; // FIXME should be no longer needed
  private final AbstractFileObject<ConnectionFileSystem> resolvedFileObject;
  private final String domain;
  private final PocVFSUriTransformer vfsUriTransformer;

  /**
   * @deprecated POC should not use going forward instead use
   * {@link #ConnectionFileObject(AbstractFileName, ConnectionFileSystem, AbstractFileObject, String, PocVFSUriTransformer)}
   * @param name
   * @param fs
   * @param resolvedFileObject
   * @param domain
   */
  public ConnectionFileObject( AbstractFileName name, ConnectionFileSystem fs,
                               AbstractFileObject<ConnectionFileSystem> resolvedFileObject,
                               String domain ) {
    super( name, fs );
    this.resolvedFileObject = resolvedFileObject;
    this.domain = domain;
    this.vfsUriTransformer = new PocLegacyGetChildVFSUriTransformer( this, domain ); // handle backwards compatibility
  }

  public ConnectionFileObject( AbstractFileName name, ConnectionFileSystem fs,
                               AbstractFileObject<ConnectionFileSystem> resolvedFileObject,
                               String domain, PocVFSUriTransformer vfsUriTransformer ) {
    super( name, fs );
    this.resolvedFileObject = resolvedFileObject;
    this.domain = domain;
    this.vfsUriTransformer = vfsUriTransformer;
  }


  @Override protected long doGetContentSize() throws Exception {
    return 0;
  }

  @Override protected InputStream doGetInputStream() throws Exception {
    return null;
  }

  @Override protected FileType doGetType() throws Exception {
    return null;
  }

  @Override protected String[] doListChildren() throws Exception {
    return new String[ 0 ];
  }

  @Override public boolean canRenameTo( FileObject newfile ) {
    return resolvedFileObject.canRenameTo( newfile );
  }

  @Override public void close() throws FileSystemException {
    resolvedFileObject.close();
  }

  @Override public void copyFrom( FileObject file, FileSelector selector ) throws FileSystemException {
    resolvedFileObject.copyFrom( file, selector );
  }

  @Override public void createFile() throws FileSystemException {
    resolvedFileObject.createFile();
  }

  @Override public void createFolder() throws FileSystemException {
    resolvedFileObject.createFolder();
  }

  @Override public boolean delete() throws FileSystemException {
    return resolvedFileObject.delete();
  }

  @Override public int delete( FileSelector selector ) throws FileSystemException {
    return resolvedFileObject.delete( selector );
  }

  @Override public int deleteAll() throws FileSystemException {
    return resolvedFileObject.deleteAll();
  }

  @Override public boolean exists() throws FileSystemException {
    if ( resolvedFileObject == null ) {
      return false;
    }
    return resolvedFileObject.exists();
  }

  @Override public FileObject getChild( String name ) throws FileSystemException {
    return getChild( resolvedFileObject.getChild( name ) );
  }

  /**
   * Resolve children from the delegated vfs provider then convert to PVFS
   *
   * @return File objects with PVFS scheme
   * @throws FileSystemException File doesn't exist
   */
  @Override public FileObject[] getChildren() throws FileSystemException {
    FileObject[] resolvedChildren = resolvedFileObject.getChildren();
    FileObject[] children = new FileObject[ resolvedChildren.length ];
    for ( int i = 0; i < resolvedChildren.length; i++ ) {
      children[ i ] = getChild( resolvedChildren[ i ] );
    }
    return children;
  }

  /**
   * Convert resolved file object to PVFS scheme
   *
   * @param fileObject The resolve FileObject
   * @return PVFS scheme FileObject
   * @throws FileSystemException File doesn't exist
   */
  private FileObject getChild( FileObject fileObject ) throws FileSystemException {
    String connectionPath = vfsUriTransformer.toVfsUri( fileObject );
    return this.resolveFile( connectionPath );
  }

  @Override public FileContent getContent() throws FileSystemException {
    return resolvedFileObject.getContent();
  }

  @Override public FileOperations getFileOperations() throws FileSystemException {
    return resolvedFileObject.getFileOperations();
  }

  @Override public InputStream getInputStream() throws FileSystemException {
    return resolvedFileObject.getInputStream();
  }

  @Override public OutputStream getOutputStream() throws FileSystemException {
    return resolvedFileObject.getOutputStream();
  }

  @Override public OutputStream getOutputStream( boolean bAppend ) throws FileSystemException {
    return resolvedFileObject.getOutputStream( bAppend );
  }

  @Override public RandomAccessContent getRandomAccessContent( RandomAccessMode mode ) throws FileSystemException {
    return resolvedFileObject.getRandomAccessContent( mode );
  }

  @Override public FileType getType() throws FileSystemException {
    return resolvedFileObject.getType();
  }

  @Override public void holdObject( Object strongRef ) {
    resolvedFileObject.holdObject( strongRef );
  }

  @Override public boolean isAttached() {
    return resolvedFileObject.isAttached();
  }

  @Override public boolean isContentOpen() {
    return resolvedFileObject.isContentOpen();
  }

  @Override public boolean isExecutable() throws FileSystemException {
    return resolvedFileObject.isExecutable();
  }

  @Override public boolean isFile() throws FileSystemException {
    return resolvedFileObject.isFile();
  }

  @Override public boolean isFolder() throws FileSystemException {
    return resolvedFileObject.isFolder();
  }

  @Override public boolean isHidden() throws FileSystemException {
    return resolvedFileObject.isHidden();
  }

  @Override public boolean isReadable() throws FileSystemException {
    return resolvedFileObject.isReadable();
  }

  @Override public boolean isWriteable() throws FileSystemException {
    return resolvedFileObject.isWriteable();
  }

  @Override public void moveTo( FileObject destFile ) throws FileSystemException {
    resolvedFileObject.moveTo( destFile );
  }

  @Override public void refresh() throws FileSystemException {
    if ( resolvedFileObject != null ) {
      resolvedFileObject.refresh();
    }
  }

  public FileObject getResolvedFileObject() {
    return resolvedFileObject;
  }

  @Override
  public String getOriginalURIString() {
    return this.getName().toString();
  }

  @Override
  public String getAELSafeURIString() {
    return resolvedFileObject.getPublicURIString().replaceFirst( "s3://", "s3a://" );
  }
}
