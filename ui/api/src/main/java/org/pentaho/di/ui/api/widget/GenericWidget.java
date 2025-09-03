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
 * Represents a generic UI widget.
 *
 * @param <T> the type of the underlying widget implementation
 */
public interface GenericWidget<T> {
  /**
   * Returns the underlying widget implementation.
   *
   * @return the underlying widget
   */
  T getUnderlyingWidget();
}
