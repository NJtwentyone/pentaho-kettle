package org.pentaho.di.ui.core.widget.adapter;

import org.eclipse.swt.widgets.Group;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SwtGroupAdapterTest {

  private Group mockGroup;
  private SwtGroupAdapter adapter;

  @Before
  public void setUp() {
    mockGroup = Mockito.mock( Group.class );
    adapter = new SwtGroupAdapter( mockGroup );
  }

  @Test
  public void testConstructorStoresGroup() {
    Assert.assertSame( mockGroup, adapter.getUnderlyingWidget() );
  }

  @Test
  public void testGetTextDelegatesToGroup() {
    Mockito.when( mockGroup.getText() ).thenReturn( "Test Label" );
    Assert.assertEquals( "Test Label", adapter.getText() );
    Mockito.verify( mockGroup ).getText();
  }

  @Test
  public void testSetTextDelegatesToGroup() {
    adapter.setText( "New Label" );
    Mockito.verify( mockGroup ).setText( "New Label" );
  }

  @Test
  public void testGetUnderlyingWidgetReturnsGroup() {
    Assert.assertSame( mockGroup, adapter.getUnderlyingWidget() );
  }
}
