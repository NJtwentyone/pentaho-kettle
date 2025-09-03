/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.di.ui.api.widget;

import org.junit.Test;
import org.junit.Assert;

public class ButtonWidgetTest {

  private static class DummyButtonWidget implements ButtonWidget<Object> {
    private boolean enabled = true;
    private boolean clicked = false;
    private Object value = null;

    @Override
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public void setEnabled( boolean enabled ) {
      this.enabled = enabled;
    }

    @Override
    public void click() {
      if ( enabled ) {
        clicked = true;
      }
    }

    // Implement getUnderlyingWidget from GenericWidget
    @Override
    public Object getUnderlyingWidget() {
      return "dummy";
    }

    public boolean wasClicked() {
      return clicked;
    }
  }

  @Test
  public void testSetAndGetEnabled() {
    DummyButtonWidget widget = new DummyButtonWidget();
    widget.setEnabled( false );
    Assert.assertFalse( widget.isEnabled() );
    widget.setEnabled( true );
    Assert.assertTrue( widget.isEnabled() );
  }

  @Test
  public void testClickWhenEnabled() {
    DummyButtonWidget widget = new DummyButtonWidget();
    widget.setEnabled( true );
    widget.click();
    Assert.assertTrue( widget.wasClicked() );
  }

  @Test
  public void testClickWhenDisabled() {
    DummyButtonWidget widget = new DummyButtonWidget();
    widget.setEnabled( false );
    widget.click();
    Assert.assertFalse( widget.wasClicked() );
  }
}
