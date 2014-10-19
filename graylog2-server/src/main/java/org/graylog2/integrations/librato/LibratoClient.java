/**
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.integrations.librato;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Realm;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LibratoClient {

    private static final Logger LOG = LoggerFactory.getLogger(LibratoClient.class);

    private final URI apiUrl;
    private final String username;
    private final String token;

    private final AsyncHttpClient asyncHttpClient;
    private final ObjectMapper objectMapper;

    private List<LibratoMetric> buffer;

    public LibratoClient(URI apiUrl, String username, String token, final AsyncHttpClient asyncHttpClient, final ObjectMapper objectMapper) {
        this.apiUrl = apiUrl;
        this.username = username;
        this.token = token;
        this.asyncHttpClient = asyncHttpClient;
        this.objectMapper = objectMapper;

        this.buffer = Lists.newArrayList();
    }

    public void addToBuffer(LibratoMetric metric) {
        this.buffer.add(metric);
    }

    public List<LibratoMetric> clearBuffer() {
        List<LibratoMetric> oldMetrics = Lists.newArrayList(this.buffer);
        this.buffer = Lists.newArrayList();

        return oldMetrics;
    }

    public void submit(List<LibratoMetric> metrics) {
        LOG.debug("Submitting <{}> metrics to Librato.", metrics.size());

        Map<String, List<Map<String, Object>>> payload = Maps.newHashMap();
        payload.put("gauges", new ArrayList<Map<String, Object>>());
        payload.put("counters", new ArrayList<Map<String, Object>>());

        for (LibratoMetric metric : metrics) {
            if(!payload.containsKey(metric.getToplevelName())) {
                LOG.error("Unsupported top level metric type [{}]. Skipping metric.", metric.getToplevelName());
                continue;
            }

            payload.get(metric.getToplevelName()).add(metric.asMap());
        }

        final Response r;
        try {
            Realm auth = new Realm.RealmBuilder()
                    .setScheme(Realm.AuthScheme.BASIC)
                    .setPrincipal(username)
                    .setPassword(token)
                    .build();

            final URL url = apiUrl.toURL();
            r = asyncHttpClient.preparePost(url.toString())
                    .setHeader("Content-Type", "application/json")
                    .setRealm(auth)
                    .setBody(objectMapper.writeValueAsString(payload))
                    .execute().get();

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize payload", e);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Malformed URL", e);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (r.getStatusCode() != 200) {
            throw new RuntimeException("Expected Librato HTTP response [200] but got [" + r.getStatusCode() + "].");
        }
    }

}
