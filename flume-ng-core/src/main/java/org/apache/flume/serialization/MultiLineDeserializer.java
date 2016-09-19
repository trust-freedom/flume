package org.apache.flume.serialization;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MultiLineDeserializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger
      (MultiLineDeserializer.class);

  private final ResettableInputStream in;
  private final Charset outputCharset;
  private final int maxLineLength;
  private final char newLogStartPrefix;
  private volatile boolean isOpen;

  public static final String OUT_CHARSET_KEY = "outputCharset";
  public static final String CHARSET_DFLT = "UTF-8";

  public static final String MAXLINE_KEY = "maxLineLength";
  public static final int MAXLINE_DFLT = 2048;
  
  public static final String NEW_LOG_PREFIX = "newLogStartPrefix";//日志开始标示
  public static final String NEW_LOG_DELT = "^";

  MultiLineDeserializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.outputCharset = Charset.forName(
        context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
    this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
    this.isOpen = true;
    this.newLogStartPrefix = context.getString(NEW_LOG_PREFIX, NEW_LOG_DELT).charAt(0);
  }

  /**
   * Reads a line from a file and returns an event
   * @return Event containing parsed line
   * @throws IOException
   */
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    String line = readLog();
    if (line == null) {
      return null;
    } else {
      return EventBuilder.withBody(line, outputCharset);
    }
  }

  /**
   * Batch line read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read lines
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  // TODO: consider not returning a final character that is a high surrogate
  // when truncating
  private String readLog() throws IOException {
    StringBuilder sb = new StringBuilder();
    int c;
    int readChars = 0;
    while ((c = in.readChar()) != -1) {
      // FIXME: support \r\n
      if (c == '\n') {
    	  c = in.readChar();  
    	  
          if (c == -1) {
        	  break;  
          }
          else if (c == this.newLogStartPrefix) {
        	  break;  
          }
          else{
        	  sb.append('\n');
          }
      }
      //不记录newLogStartPrefix
      else if (c == this.newLogStartPrefix) {
    	  continue; 
      }
      
      readChars++;

//      System.out.println("+++++++++=="+(char)c);
      sb.append((char)c);

      if (readChars >= maxLineLength) {
        logger.warn("Log length exceeds max ({}), truncating log!",
            maxLineLength);
        break;
      }
    }
    
    System.out.println("readChars+++++++++++++++++"+readChars);
    if (readChars > 0) {
    	System.out.println();
    	System.out.println("================");
    	System.out.println(sb.toString());
    	System.out.println("----------------");
    	System.out.println();
      return sb.toString();
    } else {
      return null;
    }
  }

  public static class Builder implements EventDeserializer.Builder {
    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      return new MultiLineDeserializer(context, in);
    }

  }

}
