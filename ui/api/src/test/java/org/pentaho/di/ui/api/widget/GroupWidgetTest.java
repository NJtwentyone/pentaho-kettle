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

class DummyGroupWidget implements GroupWidget<String, Object> {
  private String label = "";

  @Override
  public String getText() {
    return label;
  }

  @Override
  public void setText( String label ) {
    this.label = label;
  }

  @Override
  public Object getUnderlyingWidget() {
    return "dummy";
  }
}

public class GroupWidgetTest {
  @Test
  public void testSetAndGetText() {
    DummyGroupWidget widget = new DummyGroupWidget();
    widget.setText( "label" );
    Assert.assertEquals( "label", widget.getText() );
  }

  @Test
  public void testGetUnderlyingWidget() {
    DummyGroupWidget widget = new DummyGroupWidget();
    Assert.assertEquals( "dummy", widget.getUnderlyingWidget() );
  }
}
