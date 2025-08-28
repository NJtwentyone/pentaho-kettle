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
 * Represents a text widget with get/set text functionality.
 *
 * @param <T> the type of text value
 * @param <S> the type of the underlying widget implementation
 */
public interface TextWidget<T, S> extends GenericWidget<S> {
  /**
   * Gets the text value of the widget.
   *
   * @return the text value
   */
  T getText();

  /**
   * Sets the text value of the widget.
   *
   * @param text the text value to set
   */
  void setText( T text );
}
