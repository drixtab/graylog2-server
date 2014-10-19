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
package org.graylog2.outputs.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ning.http.client.AsyncHttpClient;
import org.graylog2.integrations.librato.GaugeMetric;
import org.graylog2.integrations.librato.LibratoClient;
import org.graylog2.integrations.librato.LibratoMetric;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.google.common.base.Strings.isNullOrEmpty;

public class LibratoOutput implements MessageOutput {

    private static final Logger LOG = LoggerFactory.getLogger(LibratoOutput.class);

    public static final String NAME = "Librato Metrics Output (EXPERIMENTAL)";

    public static final String LIBRATO_URL = "https://metrics-api.librato.com/v1/metrics";

    private boolean isRunning = false;

    public static final String CK_USERNAME = "username";
    public static final String CK_TOKEN = "token";
    public static final String CK_FIELDS = "fields";
    public static final String CK_SOURCE = "source";
    public static final String CK_METRIC_PREFIX = "prefix";

    private List<String> gaugeFields;
    private String source;

    private final AsyncHttpClient asyncHttpClient;
    private final ObjectMapper objectMapper;

    private LibratoClient client;

    private Configuration config;

    private ScheduledExecutorService submitService;

    private static final int RUN_RATE = 10; // TODO

    @Inject
    public LibratoOutput(AsyncHttpClient asyncHttpClient, ObjectMapper objectMapper) {
        this.asyncHttpClient = asyncHttpClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public void initialize(Configuration config) throws MessageOutputConfigurationException {
        if (!checkConfiguration(config)) {
            throw new MessageOutputConfigurationException("Missing configuration parameters.");
        }

        this.submitService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("librato-submitter-%d").build()
        );

        this.submitService.scheduleWithFixedDelay(submitMetrics(), RUN_RATE, RUN_RATE, TimeUnit.SECONDS);

        this.config = config;

        this.client = new LibratoClient(
                URI.create(LIBRATO_URL),
                config.getString(CK_USERNAME),
                config.getString(CK_TOKEN),
                asyncHttpClient,
                objectMapper
        );

        this.gaugeFields = Arrays.asList(config.getString(CK_FIELDS).split(","));
        this.source = config.getString(CK_SOURCE);

        isRunning = true;
    }

    private boolean checkConfiguration(Configuration config) {
        if (isNullOrEmpty(config.getString(CK_USERNAME))
                || isNullOrEmpty(config.getString(CK_TOKEN))
                || isNullOrEmpty(config.getString(CK_FIELDS))
                || isNullOrEmpty(config.getString(CK_SOURCE))) {
            return false;
        }

        return true;
    }

    @Override
    public void stop() {
        this.submitService.shutdown();
        this.isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return this.isRunning;
    }

    @Override
    public void write(Message message) throws Exception {
        for (String field : gaugeFields) {
            field = field.trim();

            LOG.trace("Trying to read field [{}] from message <{}>.", field, message.getId());
            if(!message.getFields().containsKey(field)) {
                LOG.debug("Message <{}> does not contain field [{}]. Not sending to Librato.", message.getId(), field);
                continue;
            }

            // Get value.
            Object messageValue = message.getField(field);
            Number metricValue;
            if (messageValue instanceof Long) {
                metricValue = (Long) messageValue;
            } else if(messageValue instanceof Integer) {
                metricValue = (Integer) messageValue;
            } else if(messageValue instanceof Float) {
                metricValue = (Float) messageValue;
            } else if(messageValue instanceof Double) {
                metricValue = (Double) messageValue;
            } else {
                LOG.debug("Field [{}] of message <{}> is not of numeric type. Not sending to Librato. (Type was: [{}])",
                        field, message.getId(), messageValue.getClass().getCanonicalName());
                continue;
            }

            // Get timestamp.
            DateTime timestamp = (DateTime) message.getField(Message.FIELD_TIMESTAMP);
            int unixTimestamp = (int) ((timestamp.getMillis())/1000);

            client.addToBuffer(new GaugeMetric(metricName(field), metricValue, unixTimestamp, this.source));
        }
    }

    private String metricName(String field) {
        if (!isNullOrEmpty(config.getString(CK_METRIC_PREFIX))) {
            return config.getString(CK_METRIC_PREFIX) + "." + field;
        }

        return field;
    }

    private Runnable submitMetrics() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    client.submit(compact(client.clearBuffer()));
                } catch (Exception e) {
                    LOG.error("Could not submit metrics to Librato.", e);
                }
            }
        };
    }

    private List<LibratoMetric> compact(List<LibratoMetric> metrics) {
        List<LibratoMetric> compacted = Lists.newArrayList();

        Map<String, List<LibratoMetric>> buckets = Maps.newHashMap();

        // Sort metrics by name.
        for (LibratoMetric metric : metrics) {
            if (!buckets.containsKey(metric.getName())) {
                buckets.put(metric.getName(), new ArrayList<LibratoMetric>());
            }

            buckets.get(metric.getName()).add(metric);
        }

        for (Map.Entry<String, List<LibratoMetric>> bucket : buckets.entrySet()) {
            int bucketSize = bucket.getValue().size();
            if (bucketSize == 0) {
                continue;
            }

            LOG.debug("Compacting metrics bucket [{}] with {} values.", bucket.getKey(), bucketSize);

            double compactResult = mean(bucket.getValue(), bucketSize);

            LOG.debug("Compacting result of bucket [{}]: <{}>", bucket.getKey(), compactResult);

            String type = bucket.getValue().get(0).getToplevelName();
            if (type.equals("gauges")) {
                compacted.add(new GaugeMetric(bucket.getKey(), compactResult, Tools.getUTCTimestamp(), this.source));
            }
        }

        return compacted;
    }

    public double mean(List<LibratoMetric> metrics, int size) {
        if (size == 0 || metrics == null) {
            return 0;
        }

        double sum = 0;
        for (LibratoMetric metric : metrics) {
            sum += metric.getValue().doubleValue();
        }

        return sum / size;
    }

    @Override
    public void write(List<Message> messages) throws Exception {
        // Messages are batched in client.
        for (Message message : messages) {
            write(message);
        }
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        ConfigurationRequest r = new ConfigurationRequest();

        r.addField(new TextField(
                CK_USERNAME,
                "Username",
                "",
                "Your Librato username or email address",
                ConfigurationField.Optional.NOT_OPTIONAL
        ));

        r.addField(new TextField(
                CK_TOKEN,
                "API token",
                "",
                "Your Librato API token",
                ConfigurationField.Optional.NOT_OPTIONAL,
                TextField.Attribute.IS_PASSWORD
        ));

        r.addField(new TextField(
                CK_SOURCE,
                "Source to report to Librato",
                "graylog2",
                "",
                ConfigurationField.Optional.NOT_OPTIONAL
        ));

        r.addField(new TextField(
                CK_METRIC_PREFIX,
                "Prefix for metric name",
                "",
                "A field called \"response_time\"would be transmitted as \"prefix.reponse_time\"",
                ConfigurationField.Optional.OPTIONAL
        ));

        r.addField(new TextField(
                CK_FIELDS,
                "Message fields to submit to Librato",
                "response_time,db_time,view_time",
                "A comma separated list of field values in messages that should be transmitted to Librato as gauge values.",
                ConfigurationField.Optional.NOT_OPTIONAL
        ));

        return r;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getHumanName() {
        return NAME;
    }

    @Override
    public String getLinkToDocs() {
        return "http://www.graylog2.org/";
    }

}
