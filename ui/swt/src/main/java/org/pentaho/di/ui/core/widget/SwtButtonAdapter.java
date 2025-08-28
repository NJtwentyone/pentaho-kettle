package org.pentaho.di.ui.core.widget;

import org.pentaho.di.ui.api.widget.ButtonWidget;
import org.eclipse.swt.widgets.Button;

public class SwtButtonAdapter implements ButtonWidget<Button> {

  protected final Button mbutton;

  public SwtButtonAdapter( Button button ) {
    this.mbutton = button;
  }

  @Override public boolean isEnabled() {
    return this.mbutton.isEnabled();
  }

  @Override public void setEnabled( boolean enabled ) {
    this.mbutton.setEnabled( enabled );
  }

  @Override public void click() {
    // TODO do nothing
  }

  @Override public Button getUnderlyingWidget() {
    return mbutton;
  }
}
