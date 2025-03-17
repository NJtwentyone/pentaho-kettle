package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;

/**
 * Going with naming convention {INTERFACE}Impl to match current package classes.
 * Othe implemenations of TransFactory should follow {Description}{{INTERFACE} ie RemoteTransFactory.java
 */
public class TransFactoryImpl implements TransFactory {
  @Override public Trans create( TransMeta transMeta, LoggingObjectInterface log,
                                 TransExecutionConfiguration transExecutionConfiguration ) throws KettleException {
    // transExecutionConfiguration  is always "Pentaho Local" from DefaultRunConfigurationProvider# DEFAULT_CONFIG_NAME
    return new Trans(transMeta, log);
  }
}
