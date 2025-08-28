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

/**
 * Represents a generic button widget with enable/disable and click functionality.
 *
 * @param <T> the type of value associated with the widget
 */
public interface ButtonWidget<T> extends GenericWidget<T> {

  /**
   * Checks if the button is enabled.
   *
   * @return true if the button is enabled, false otherwise
   */
  boolean isEnabled();

  /**
   * Sets the enabled state of the button.
   *
   * @param enabled true to enable the button, false to disable it
   */
  void setEnabled( boolean enabled );

  /**
   * Simulates a click action on the button.
   */
  void click();
}
