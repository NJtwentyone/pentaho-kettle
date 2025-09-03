package org.pentaho.di.ui.core.widget.adapter;

import org.eclipse.swt.widgets.Button;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SwtButtonAdapterTest {

  private Button mockButton;
  private SwtButtonAdapter adapter;

  @Before
  public void setUp() {
    mockButton = mock( Button.class );
    adapter = new SwtButtonAdapter( mockButton );
  }

  @Test
  public void testIsEnabled() {
    when( mockButton.isEnabled() ).thenReturn( true );
    Assert.assertTrue( adapter.isEnabled() );
    when( mockButton.isEnabled() ).thenReturn( false );
    Assert.assertFalse( adapter.isEnabled() );
  }

  @Test
  public void testSetEnabled() {
    adapter.setEnabled( true );
    verify( mockButton ).setEnabled( true );
    adapter.setEnabled( false );
    verify( mockButton ).setEnabled( false );
  }

  @Test
  public void testClickDoesNothing() {
    adapter.click();
    // No interaction expected
    verifyNoMoreInteractions( mockButton );
  }

  @Test
  public void testGetUnderlyingWidget() {
    Assert.assertEquals( mockButton, adapter.getUnderlyingWidget() );
  }
}
