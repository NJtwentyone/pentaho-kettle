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

import org.junit.Assert;
import org.junit.Test;

class DummyTextWidget implements TextWidget<String, Object> {
  private String text = "";

  @Override
  public String getText() {
    return text;
  }

  @Override
  public void setText( String text ) {
    this.text = text;
  }

  @Override
  public Object getUnderlyingWidget() {
    return "dummy";
  }
}

public class TextWidgetTest {
  @Test
  public void testSetAndGetText() {
    DummyTextWidget widget = new DummyTextWidget();
    widget.setText( "abc" );
    Assert.assertEquals( "abc", widget.getText() );
  }

  @Test
  public void testGetUnderlyingWidget() {
    DummyTextWidget widget = new DummyTextWidget();
    Assert.assertEquals( "dummy", widget.getUnderlyingWidget() );
  }
}
