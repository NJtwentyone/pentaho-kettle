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


package org.pentaho.di.ui.core.widget.adapter;

import org.eclipse.swt.widgets.Text;
import org.pentaho.di.ui.api.widget.TextWidget;

/**
 * SWT implementation of the TextWidget interface.
 */
public class SwtTextAdapter implements TextWidget<String, Text> {

  protected final Text text;

  /**
   * Constructs a SwtTextAdapter for the given SWT Text widget.
   *
   * @param swtText the SWT Text widget
   */
  public SwtTextAdapter( Text swtText ) {
    text = swtText;
  }

  /**
   * Sets the text value of the widget.
   *
   * @param text the text value to set
   */
  @Override public void setText( String text ) {
    this.text.setText( text );
  }

  /**
   * Gets the text value of the widget.
   *
   * @return the text value
   */
  @Override public String getText() {
    return text.getText();
  }

  /**
   * Gets the underlying SWT Text widget.
   *
   * @return the SWT Text widget
   */
  @Override public Text getUnderlyingWidget() {
    return text;
  }
}
