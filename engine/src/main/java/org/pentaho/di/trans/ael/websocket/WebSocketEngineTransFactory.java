package org.pentaho.di.trans.ael.websocket;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransFactory;
import org.pentaho.di.trans.TransMeta;

import java.net.URI;

public class WebSocketEngineTransFactory implements TransFactory {
  @Override public Trans create( TransMeta transMeta, LoggingObjectInterface log,
                                 TransExecutionConfiguration transExecutionConfiguration ) throws KettleException {
    /*
    TODO copy logic getting and parsing "engine.scheme", "engine.url" , etc...
      sources:
       - SparkRunConfigurationExecutor#execute branch 'spark-execution'
       -- https://github.com/pentaho/pentaho-kettle/blob/378d23758aeb053df583140de13e294eb19af590/plugins/engine-configuration/impl/src/main/java/org/pentaho/di/engine/configuration/impl/spark/SparkRunConfigurationExecutor.java#L61
       - TransSupplier#get branch 'spark-execution'
       -- https://github.com/pentaho/pentaho-kettle/blob/378d23758aeb053df583140de13e294eb19af590/engine/src/main/java/org/pentaho/di/trans/TransSupplier.java#L58
     */
    URI uri = URI.create( "localhost:53000" );
    boolean ssl = false;
    // not sure this log line is needed here or if there is a better way than propagating  LogChannelInterface everywhere
    //log.logBasic( BaseMessages.getString( PKG, MSG_SPARK_ENGINE, protocol, url ) );
    return new TransWebSocketEngineAdapter( transMeta, uri.getHost(), uri.getPort(), ssl );
  }
}
