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

import org.pentaho.di.engine.configuration.api.CheckedMetaStoreSupplier;
import org.pentaho.di.engine.configuration.api.RunConfiguration;
import org.pentaho.di.engine.configuration.api.RunConfigurationService;
import org.pentaho.di.engine.configuration.impl.RunConfigurationManager;
import org.pentaho.di.engine.configuration.impl.pentaho.DefaultRunConfiguration;

/**
 * Factory class to create RunConfigurationService instances without direct dependencies
 */
public class RunConfigurationServiceFactory {

  public RunConfigurationService createRunConfigurationService(CheckedMetaStoreSupplier supplier) {
    return RunConfigurationManager.getInstance(supplier);
  }

  public RunConfiguration createDefaultRunConfiguration() {
    return new DefaultRunConfiguration();
  }
}
