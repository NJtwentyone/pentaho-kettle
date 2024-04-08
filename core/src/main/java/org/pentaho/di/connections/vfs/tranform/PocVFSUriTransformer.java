package org.pentaho.di.connections.vfs.tranform;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.di.connections.vfs.provider.ConnectionFileObject;
import org.pentaho.di.connections.vfs.provider.ConnectionFileSystem;

/**
 * POC class to demonstrate how to encapsulate logic for:
 * PVFS URI <-->  Provider URI (also called NATIVE URI or PHYSICAL URI)
 * Simple Example:
 * PVFS URI - "pvfs://someConnectionName/someLargeBucket/someDir1/anotherFileB/randomFile1.txt
 * Provider - URI "s3://someLargeBucket/someDir1/anotherFileB/randomFile1.txt"
 *
 * I don't want to add a whole VFSConnectionDetails object reference in ConnectionFileObject#getChild
 * TODO look if there is a way to just pass around a lamda or some other approach with a light impact
 * I don't want references to VFSConnectionDetails  stored in a FileObjets that is never used
 */
public interface PocVFSUriTransformer {

  /**
   * GOAL: replace {@link ConnectionFileSystem#getUrl}
   * PVFS URI --> Provider URI
   * @param abstractFileName
   * @return
   */
    String toProviderUri( AbstractFileName abstractFileName); // TODO update code to throw some type of exception

  /***
   * GOAL: replace {@link ConnectionFileObject#getChildren} helper function #getChild conversion logic
   * PVFS URI <-- Provider URI
   * @param fileObject
   * @return
   */
    String toVfsUri ( FileObject fileObject ); // TODO update code to throw some type of exception

}
