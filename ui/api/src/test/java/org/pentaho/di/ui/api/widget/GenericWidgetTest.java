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

class DummyWidget implements GenericWidget<String> {
  @Override
  public String getUnderlyingWidget() {
    return "dummy";
  }
}

public class GenericWidgetTest {
  @Test
  public void testGetUnderlyingWidget() {
    GenericWidget<String> widget = new DummyWidget();
    Assert.assertEquals("dummy", widget.getUnderlyingWidget() );
  }
}
