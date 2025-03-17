package org.pentaho.di.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;

import java.util.HashMap;
import java.util.Map;

// this class's logic may make sense to merge with other classes
public class DefaultTransManager implements TransMananger {

  private static final DefaultTransManager instance = new DefaultTransManager();

  Map<String, TransFactory> mapConfigurationTypeTransFactory;


  public DefaultTransManager(){
    mapConfigurationTypeTransFactory = new HashMap<>();
    // TODO make field DefaultRunConfiguration#TYPE 'final'
    // TODO maybe auto load key/value pair "Pentaho" -> TransFactory
    /*
    registerFactory(DefaultRunConfiguration.TYPE, new TransFactoryImpl());
     */
    // NOTE all plugins would call #registerFactory
  }

  public static DefaultTransManager getInstance() {
    return instance;
  }

  @Override
  public Trans createTrans( TransMeta transMeta, LoggingObjectInterface log,
                                      TransExecutionConfiguration transExecutionConfiguration ) throws KettleException {
    // TODO null checks for arguments
    TransFactory transFactory = getTransFactory( transExecutionConfiguration.getRunConfiguration() );
    return transFactory.create( transMeta, log, transExecutionConfiguration );
  }

  @Override
  public boolean registerFactory( String runConfigurationType, TransFactory transFactory ) {
    // TODO null checks for arguments
    // TODO not sure if this is useful to return boolean to indicate if a new entry
    return mapConfigurationTypeTransFactory.put( runConfigurationType, transFactory ) != null;

  }

  @Override public TransFactory getTransFactory( String runConfigurationType ) {
    // TODO null checks for arguments
    // TODO log if run configuration is not found
    // TODO TransFactoryImpl()  is considered default maybe add to class java constant
    return mapConfigurationTypeTransFactory.getOrDefault(runConfigurationType, new TransFactoryImpl()  );
  }
}
