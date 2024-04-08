package org.pentaho.di.connections.vfs.tranform;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.URLFileName;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.di.connections.vfs.provider.ConnectionFileProvider;

public class PocDefaultVFSUriTransformer implements PocVFSUriTransformer {

  public static final String DOMAIN_ROOT = "[\\w]+://";

  final VFSConnectionDetails vfsConnectionDetails;

  public PocDefaultVFSUriTransformer( VFSConnectionDetails vfsConnectionDetails ) {
    this.vfsConnectionDetails = vfsConnectionDetails;
  }

  @Override
  public String toProviderUri( AbstractFileName abstractFileName ) { // FIXME use an URI Builder java.net.URI(String scheme, String authority, String path, String query, String fragment) or other options - https://www.baeldung.com/java-url
    String url = null;

    String domain = vfsConnectionDetails.getDomain();
    if ( !domain.equals( "" ) ) {
      domain = "/" + domain;
    }
    url = vfsConnectionDetails.getType() + ":/" + domain + abstractFileName.getPath();
    //TODO Looks like a bug. For now excluding this for connections with hasBuckets. For future, needs to be re-analyzed.
    if ( url.matches( DOMAIN_ROOT ) && vfsConnectionDetails.hasBuckets() ) { // FIXME ConnectionDetails#hasBuckets should be moved down to VFSConnectionDetails or lower
      url += vfsConnectionDetails.getName();
    }

    return url;
  }

  @Override
  public String toVfsUri( FileObject fileObject ) { // FIXME use an URI Builder java.net.URI(String scheme, String authority, String path, String query, String fragment) or other options - https://www.baeldung.com/java-url
    try {
      //     String connectionName = ( (ConnectionFileName) this.getName() ).getConnection();
      String connectionName = vfsConnectionDetails.getName(); // TODO check this
      StringBuilder connectionPath = new StringBuilder();
      connectionPath.append( ConnectionFileProvider.SCHEME );
      connectionPath.append( "://" );
      connectionPath.append( connectionName );
      connectionPath.append( "/" );
      // if ( domain == null || domain.equals( "" ) ) {
      if ( vfsConnectionDetails.getDomain() == null || vfsConnectionDetails.getDomain().equals( "" ) ) { // TODO check this
        // S3 does not return a URLFleName; but Google does hence the difference here
        if ( fileObject.getName() instanceof URLFileName ) {
          URLFileName urlFileName = (URLFileName) fileObject.getName();
          connectionPath.append( urlFileName.getHostName() );
        } else {
          connectionPath.append( fileObject.getURL().getHost() );
        }
      }
      connectionPath.append( fileObject.getName().getPath() );

      return connectionPath.toString();
    } catch ( FileSystemException fse ) {
      throw new RuntimeException(fse); // TODO better exception handling
    }
  }
}
