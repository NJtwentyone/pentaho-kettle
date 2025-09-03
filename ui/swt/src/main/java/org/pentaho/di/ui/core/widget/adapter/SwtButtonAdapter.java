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

import org.pentaho.di.ui.api.widget.ButtonWidget;
import org.eclipse.swt.widgets.Button;

/**
 * SWT implementation of the ButtonWidget interface.
 */
public class SwtButtonAdapter implements ButtonWidget<Button> {

  protected final Button mbutton;

  /**
   * Constructs a SwtButtonAdapter for the given SWT Button.
   *
   * @param button the SWT Button widget
   */
  public SwtButtonAdapter( Button button ) {
    this.mbutton = button;
  }

  /**
   * Checks if the button is enabled.
   *
   * @return true if enabled, false otherwise
   */
  @Override public boolean isEnabled() {
    return this.mbutton.isEnabled();
  }

  /**
   * Sets the enabled state of the button.
   *
   * @param enabled true to enable, false to disable
   */
  @Override public void setEnabled( boolean enabled ) {
    this.mbutton.setEnabled( enabled );
  }

  /**
   * Simulates a click action on the button.
   * (Currently does nothing.)
   */
  @Override public void click() {
    // Do nothing SWT uses SelectionListener to handle button clicks
  }

  /**
   * Gets the underlying SWT Button widget.
   *
   * @return the SWT Button widget
   */
  @Override public Button getUnderlyingWidget() {
    return mbutton;
  }
}
