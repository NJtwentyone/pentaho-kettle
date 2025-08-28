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


package org.pentaho.di.engine.ui;

import org.pentaho.di.engine.configuration.api.RunConfigurationDialog;
import org.pentaho.di.ui.core.widget.adapter.SwtButtonAdapter;
import org.pentaho.di.ui.core.widget.adapter.SwtGroupAdapter;
import org.pentaho.di.ui.core.widget.adapter.SwtTextAdapter;

/**
 * SWT-specific extension of the RunConfigurationDialog interface.
 * Provides access to SWT widget adapters for dialog controls.
 */
public interface SwtRunConfigurationDialog extends RunConfigurationDialog {
  /**
   * Gets the name text widget adapter.
   *
   * @return the SwtTextAdapter for the name field
   */
  SwtTextAdapter getName();

  /**
   * Gets the group widget adapter.
   *
   * @return the SwtGroupAdapter for the group
   */
  SwtGroupAdapter getGroup();

  /**
   * Gets the OK button widget adapter.
   *
   * @return the SwtButtonAdapter for the OK button
   */
  SwtButtonAdapter getOKButton(); //org.pentaho.di.ui.core.widget.InputButton

  /**
   * Gets the Cancel button widget adapter.
   *
   * @return the SwtButtonAdapter for the Cancel button
   */
  SwtButtonAdapter getCancelButton();
}
