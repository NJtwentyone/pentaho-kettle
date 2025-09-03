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

import org.eclipse.swt.widgets.Group;
import org.pentaho.di.ui.api.widget.GroupWidget;

/**
 * SWT implementation of the GroupWidget interface.
 */
public class SwtGroupAdapter implements GroupWidget<String, Group> {

  protected final Group group;

  /**
   * Constructs a SwtGroupAdapter for the given SWT Group.
   *
   * @param group the SWT Group widget
   */
  public SwtGroupAdapter( Group group ) {
    this.group = group;
  }

  /**
   * Gets the text label of the group.
   *
   * @return the group label text
   */
  @Override public String getText() {
    return this.group.getText();
  }

  /**
   * Sets the text label of the group.
   *
   * @param text the label text to set
   */
  @Override public void setText( String text ) {
    this.group.setText( text );
  }

  /**
   * Gets the underlying SWT Group widget.
   *
   * @return the SWT Group widget
   */
  @Override public Group getUnderlyingWidget() {
    return group;
  }
}
