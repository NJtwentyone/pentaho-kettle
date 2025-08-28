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


package org.pentaho.di.engine.configuration.api;

import org.pentaho.di.ui.api.widget.ButtonWidget;
import org.pentaho.di.ui.api.widget.GroupWidget;
import org.pentaho.di.ui.api.widget.TextWidget;

/**
 * Interface for Run Configuration Dialogs.
 * Provides access to UI widgets for configuring and running Pentaho jobs.
 */
public interface RunConfigurationDialog {
  /**
   * Gets the TextWidget representing the name field.
   *
   * @return the name TextWidget
   */
  TextWidget getName();

  /**
   * Gets the GroupWidget representing the group section.
   *
   * @return the group GroupWidget
   */
  GroupWidget getGroup();

  /**
   * Gets the OK ButtonWidget.
   *
   * @return the OK ButtonWidget
   */
  ButtonWidget getOKButton();

  /**
   * Gets the Cancel ButtonWidget.
   *
   * @return the Cancel ButtonWidget
   */
  ButtonWidget getCancelButton();
}
