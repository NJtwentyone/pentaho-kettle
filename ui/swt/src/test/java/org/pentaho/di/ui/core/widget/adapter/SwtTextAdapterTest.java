package org.pentaho.di.ui.core.widget.adapter;

import org.eclipse.swt.widgets.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SwtTextAdapterTest {

  private Text mockText;
  private SwtTextAdapter adapter;

  @Before
  public void setUp() {
    mockText = Mockito.mock( Text.class );
    adapter = new SwtTextAdapter( mockText );
  }

  @Test
  public void testSetTextDelegatesToText() {
    adapter.setText( "Hello World" );
    Mockito.verify( mockText ).setText( "Hello World" );
  }

  @Test
  public void testGetTextDelegatesToText() {
    Mockito.when( mockText.getText() ).thenReturn( "Hello World" );
    Assert.assertEquals( "Hello World", adapter.getText() );
    Mockito.verify( mockText ).getText();
  }

  @Test
  public void testGetUnderlyingWidgetReturnsText() {
    Assert.assertSame( mockText, adapter.getUnderlyingWidget() );
  }
}
