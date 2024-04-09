package org.pentaho.di.connections.vfs;


import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.di.connections.vfs.tranform.PocDefaultVFSUriTransformer;
import org.pentaho.di.connections.vfs.tranform.PocVFSUriTransformer;

/**
 * POC Class, all new providers should extend this
 */
public abstract class BaseVFSConnectionDetails implements PocVFSUriTransformer, VFSConnectionDetails {

  /**
   * {@link PocVFSUriTransformer#toProviderUri(AbstractFileName)}
   * @param abstractFileName
   * @return
   */
  @Override
  public String toProviderUri( AbstractFileName abstractFileName ) {
    return new PocDefaultVFSUriTransformer(  this)
      .toProviderUri( abstractFileName );
  }

  /**
   * {@link PocVFSUriTransformer#toVfsUri(FileObject)}
   * @param fileObject
   * @return
   */
  @Override
  public String toVfsUri ( FileObject fileObject ) {
    return new PocDefaultVFSUriTransformer( this )
      .toVfsUri( fileObject );
  }
}
