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
 * Created by bmorrise on 8/22/17.
 */
public interface RunConfigurationDialog {
  TextWidget getName();
  // Engine Dropdown ??
  GroupWidget getGroup();
  ButtonWidget getOKButton();
  ButtonWidget getCancelButton();
}
