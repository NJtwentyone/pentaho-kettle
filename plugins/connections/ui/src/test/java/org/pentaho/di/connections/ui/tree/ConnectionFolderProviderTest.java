package org.pentaho.di.connections.ui.tree;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.extension.ExtensionPointHandler;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( PowerMockRunner.class )
@PowerMockIgnore( "jdk.internal.reflect.*" )
@PrepareForTest( { Spoon.class, ExtensionPointHandler.class, ConnectionFolderProvider.class } )
public class ConnectionFolderProviderTest {
  private ConnectionFolderProvider connectionFolderProvider;

  private Spoon spoon;

  @Before
  public void setup() {
    spoon = mock( Spoon.class );

    PowerMockito.mockStatic( Spoon.class );
    when( Spoon.getInstance() ).thenReturn( spoon );
  }

  @Test
  public void createTest(){
    Repository mockRepository = mock( Repository.class );
    doReturn( mockRepository ).when( spoon ).getRepository();
    IUser user = Mockito.mock( IUser.class );
    doReturn( user ).when( mockRepository ).getUserInfo();
    Mockito.when( user.isAdmin() ).thenReturn( true );
   /* GUIResource guiResource = mock( GUIResource.class );*/
    //IMetaStore metastore = mock( IMetaStore.class );
    //MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );
    connectionFolderProvider = mock( ConnectionFolderProvider.class  );
    connectionFolderProvider.create( mock( AbstractMeta.class ), mock( TreeNode.class ) );
    verify( connectionFolderProvider ).refresh( Matchers.any() , Matchers.any() , Matchers.any() );
  }

}
