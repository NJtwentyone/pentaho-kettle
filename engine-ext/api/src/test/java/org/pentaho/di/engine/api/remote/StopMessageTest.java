package org.pentaho.di.engine.api.remote;

import org.junit.Test;

import static org.junit.Assert.*;

public class StopMessageTest {

  private static final String REQUEST_UUID = "request-uuid-123";
  private static final String REASON_PHRASE = "stop requested";

  @Test
  public void reasonPhraseConstructorSetsSuccessStatusAndDefaults() {
    StopMessage message = new StopMessage( REASON_PHRASE );

    assertNull( message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.SUCCESS, message.getStatus() );
    assertFalse( message.isSafeStop() );
  }

  @Test
  public void requestUUIDAndReasonPhraseConstructor() {
    StopMessage message = new StopMessage( REQUEST_UUID, REASON_PHRASE );

    assertEquals( REQUEST_UUID, message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.SUCCESS, message.getStatus() );
    assertFalse( message.isSafeStop() );
  }

  @Test
  public void reasonPhraseAndStatusConstructor() {
    StopMessage message = new StopMessage( REASON_PHRASE, StopMessage.Status.FAILED );

    assertNull( message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.FAILED, message.getStatus() );
    assertFalse( message.isSafeStop() );
  }

  @Test
  public void requestUUIDReasonPhraseAndStatusConstructor() {
    StopMessage message = new StopMessage( REQUEST_UUID, REASON_PHRASE, StopMessage.Status.SESSION_KILLED );

    assertEquals( REQUEST_UUID, message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.SESSION_KILLED, message.getStatus() );
    assertFalse( message.isSafeStop() );
  }

  @Test
  public void getRequestUUID() {
    assertEquals( REQUEST_UUID, new StopMessage( REQUEST_UUID, REASON_PHRASE ).getRequestUUID() );
    assertNull( new StopMessage( REASON_PHRASE ).getRequestUUID() );
  }

  @Test
  public void getReasonPhrase() {
    assertEquals( REASON_PHRASE, new StopMessage( REASON_PHRASE ).getReasonPhrase() );
    assertNull( new StopMessage( null ).getReasonPhrase() );
  }

  @Test
  public void getStatus() {
    assertEquals( StopMessage.Status.SUCCESS, new StopMessage( REASON_PHRASE ).getStatus() );
    assertEquals( StopMessage.Status.FAILED,
      new StopMessage( REASON_PHRASE, StopMessage.Status.FAILED ).getStatus() );
    assertEquals( StopMessage.Status.SESSION_KILLED,
      new StopMessage( REQUEST_UUID, REASON_PHRASE, StopMessage.Status.SESSION_KILLED ).getStatus() );
  }

  @Test
  public void isSafeStop() {
    assertFalse( new StopMessage( REASON_PHRASE ).isSafeStop() );
    assertTrue( StopMessage.builder().safeStop( true ).build().isSafeStop() );
    assertFalse( StopMessage.builder().safeStop( false ).build().isSafeStop() );
  }

  @Test
  public void builder() {
    assertNotNull( StopMessage.builder() );

    StopMessage message = StopMessage.builder()
      .requestUUID( REQUEST_UUID )
      .reasonPhrase( REASON_PHRASE )
      .result( StopMessage.Status.FAILED )
      .safeStop( true )
      .build();

    assertEquals( REQUEST_UUID, message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.FAILED, message.getStatus() );
    assertTrue( message.isSafeStop() );
  }

  @Test
  public void builderIsSafeStopFalseStatusSessionKilled() {
    assertNotNull( StopMessage.builder() );

    StopMessage message = StopMessage.builder()
      .requestUUID( REQUEST_UUID )
      .reasonPhrase( REASON_PHRASE )
      .result( StopMessage.Status.SESSION_KILLED)
      .safeStop( false )
      .build();

    assertEquals( REQUEST_UUID, message.getRequestUUID() );
    assertEquals( REASON_PHRASE, message.getReasonPhrase() );
    assertEquals( StopMessage.Status.SESSION_KILLED, message.getStatus() );
    assertFalse( message.isSafeStop() );
  }

  @Test
  public void builderUsesDefaultsWhenNothingIsSet() {
    StopMessage message = StopMessage.builder().build();

    assertNull( message.getRequestUUID() );
    assertNull( message.getReasonPhrase() );
    assertNull( message.getStatus() );
    assertFalse( message.isSafeStop() );
  }
}