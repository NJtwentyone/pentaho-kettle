package org.pentaho.di.ui.core.widget;

import org.eclipse.swt.widgets.Text;
import org.pentaho.di.ui.api.widget.TextWidget;

// TODO see if you an use as super org.pentaho.di.ui.core.widget.TextVar or org.pentaho.di.ui.core.widget.Input
public class SwtTextAdapter implements TextWidget<String, Text> {

  protected final Text wText;

  public SwtTextAdapter( Text swtText ) {
    wText = swtText;
  }

  @Override public void setText( String text ) {
    wText.setText( text );
  }

  @Override public String getText() {
    return wText.getText();
  }


  @Override public Text getUnderlyingWidget() {
    return wText;
  }
}
