package org.pentaho.di.connections.vfs.tranform;

import org.apache.commons.lang3.NotImplementedException;  // TODO investigate which version to use org.apache.commons.lang.NotImplementedException ro org.apache.commons.lang3.NotImplementedException
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.di.connections.vfs.provider.ConnectionFileName;
import org.pentaho.di.connections.vfs.provider.ConnectionFileObject;
import org.pentaho.di.core.variables.VariableSpace;

/**
 * POC
 * For backwards compatibility with legacy approach.
 * All code smells are to make comparison 1:1 with original.
 */
public class PocLegacyGetChildVFSUriTransformer implements  PocVFSUriTransformer  {

  private final String connectionName;
  private final String connectionDomain;

  public PocLegacyGetChildVFSUriTransformer(ConnectionFileObject connectionFileObject, String domain ) {
    this.connectionName = ( (ConnectionFileName) connectionFileObject.getName() ).getConnection();
    this.connectionDomain = domain;
  }

  @Override
  public String toProviderUri( AbstractFileName abstractFileName ) {
    throw  new NotImplementedException(" Not implemented on purpose. Legacy code had a version of this in a different class.");
  }

  /**
   * POC
   * for backwards compatibility, only concerned
   * with {@link ConnectionFileObject#getChild(String)} } 's private function #getChild that contains the
   * logic to convert PVFS URI <-- Provider URI
   * copied code "as-is"
   *
   * @param fileObject
   * @return
   */
  @Override
  public String toVfsUri( FileObject fileObject ) {
    return new PocDefaultVFSUriTransformer( create( connectionName, connectionDomain ) ).toVfsUri( fileObject );
  }

  /**
   * POC creating object on the fly to avoid duplicating code
   * @param connectionName
   * @param connectionDomain
   * @return
   */
  protected VFSConnectionDetails create(final String connectionName, final String connectionDomain ) {
    return new VFSConnectionDetails() {

      @Override public String getDomain() {
        return connectionDomain;
      }
      @Override public String getName() {
        return connectionName;
      }

      @Override public void setName( String name ) {
        throw new NotImplementedException("not implemented in instantiated anonymous object");
      }

      @Override public String getType() {
        throw new NotImplementedException("not implemented in instantiated anonymous object");
      }

      @Override public String getDescription() {
        throw new NotImplementedException("not implemented in instantiated anonymous object");
      }

      @Override public VariableSpace getSpace() {
        throw new NotImplementedException("not implemented in instantiated anonymous object");
      }

      @Override public void setSpace( VariableSpace space ) {
        throw new NotImplementedException("not implemented in instantiated anonymous object");
      }
    };
  }
}
