/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.extension;

import io.confluent.kafkarest.KafkaRestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Configurable;
import java.io.IOException;
import java.util.List;

public class HeaderCopyExtension
        implements RestResourceExtension {

  private static final Logger log = LoggerFactory.getLogger(HeaderCopyExtension.class);

  @Override
  public void register(Configurable<?> config, KafkaRestConfig appConfig) {
    log.info("Registering");
    final String requestHeaderName =
            appConfig.getOriginalProperties().getProperty(this.getClass().getName().toLowerCase()
                    + ".request.header.name");
    log.info("requestHeaderName: " + requestHeaderName);
    config.register(new Filter(requestHeaderName));
  }

  @Override
  public void clean() {
  }

  private static class Filter implements ContainerResponseFilter {
    private final String requestHeaderName;

    public Filter(String requestHeaderName) {
      this.requestHeaderName = requestHeaderName;
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext)
            throws IOException {
      final List<String> headerValueList = requestContext.getHeaders().get(requestHeaderName);
      if (headerValueList == null) {
        log.debug("Request did not container header with name: " + requestHeaderName);
      } else {
        for (String headerValue : headerValueList) {
          log.debug("Request Header copied to response: " + requestHeaderName + ":" + headerValue);
          responseContext.getHeaders().add(requestHeaderName, headerValue);
        }
      }
    }
  }
}
