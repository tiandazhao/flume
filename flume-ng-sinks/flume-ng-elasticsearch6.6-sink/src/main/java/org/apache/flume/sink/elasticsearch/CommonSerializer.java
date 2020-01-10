package org.apache.flume.sink.elasticsearch;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields
 * </p>
 * <p>
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class CommonSerializer implements ElasticSearchEventSerializer {
  private static final Logger logger = LoggerFactory
      .getLogger(CommonSerializer.class);

  @Override
  public JSONObject getContent(Event event) throws IOException, JSONException {
    JSONObject js = new JSONObject(new String(event.getBody(), "UTF-8"));
    Map<String,String> headers = event.getHeaders();
    for (String key : headers.keySet()) {
      if (key.equals("timestamp")) {
        if (StringUtils.isBlank(js.optString("timestamp"))) {
          js.put(key, Long.parseLong(headers.get(key)));
        }
      } else {
        js.put(key, headers.get(key));
      }
    }

//    logger.info("body为：:"+js);
//    logger.info("headers为：:"+headers);
    return js;
  }

  @Override
  public void configure(Context context) {

  }

  @Override
  public void configure(ComponentConfiguration componentConfiguration) {

  }
}
