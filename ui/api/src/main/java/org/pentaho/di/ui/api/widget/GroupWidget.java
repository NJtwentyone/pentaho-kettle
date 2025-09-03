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
 * Represents a group widget with label and group access.
 *
 * @param <T> the type of label text
 * @param <S> the type of the underlying group widget
 */
public interface GroupWidget<T, S> extends GenericWidget<S> {
  /**
   * Gets the label text of the group.
   *
   * @return the label text
   */
  T getText();

  /**
   * Sets the label text of the group.
   *
   * @param label the label text to set
   */
  void setText( T label );
}
