package org.pentaho.di.ui.core.widget;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.pentaho.di.ui.api.widget.GroupWidget;

public class SwtGroupAdapter implements GroupWidget<String, Group> {

  protected final Group group;

  public SwtGroupAdapter( Group group ) {
    this.group = group;
  }

  @Override public String getText() {
    return this.group.getText();
  }

  @Override public void setText( String text ) {
    this.group.setText( text );
  }

  @Override public Group getGroup() {
    return group;
  }

  @Override public Group getUnderlyingWidget() {
    return group;
  }
}
