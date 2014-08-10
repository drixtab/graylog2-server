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
package org.graylog2.rest.resources.system.logs;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.graylog2.rest.resources.RestResource;
import org.graylog2.security.RestPermissions;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Map;

@RequiresAuthentication
@Api(value = "System/Loggers", description = "Internal Graylog2 loggers")
@Path("/system/loggers")
public class LoggersResource extends RestResource {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoggersResource.class);
    private static final Map<Level, Integer> SYSLOG_EQUIVALENTS;
    private static final Map<String, Subsystem> SUBSYSTEMS = ImmutableMap.of(
            "graylog2", new Subsystem("Graylog2", "org.graylog2", "All messages from graylog2-owned systems."),
            "indexer", new Subsystem("Indexer", "org.elasticsearch", "All messages related to indexing and searching."),
            "authentication", new Subsystem("Authentication", "org.apache.shiro", "All user authentication messages."),
            "sockets", new Subsystem("Sockets", "netty", "All messages related to socket communication.")
    );

    static {
        SYSLOG_EQUIVALENTS = ImmutableMap.<Level, Integer>builder()
                .put(Level.OFF, 0)
                .put(Level.FATAL, 0)
                .put(Level.ERROR, 3)
                .put(Level.WARN, 4)
                .put(Level.INFO, 6)
                .put(Level.DEBUG, 7)
                .put(Level.TRACE, 7)
                .put(Level.ALL, 7)
                .build();
    }

    @GET
    @Timed
    @ApiOperation(value = "List all loggers and their current levels")
    @Produces(MediaType.APPLICATION_JSON)
    public String loggers() {
        final Map<String, Object> loggerList = Maps.newHashMap();

        for (LoggerConfig loggerConfig : getLoggerConfigs()) {
            if (!isPermitted(RestPermissions.LOGGERS_READ, loggerConfig.getName())) {
                continue;
            }

            final Map<String, Object> loggerInfo = ImmutableMap.<String, Object>of(
                    "level", loggerConfig.getLevel().name().toLowerCase(),
                    "level_syslog", SYSLOG_EQUIVALENTS.get(loggerConfig.getLevel())
            );

            loggerList.put(loggerConfig.getName(), loggerInfo);
        }

        final Map<String, Object> result = ImmutableMap.of(
                "loggers", loggerList,
                "total", loggerList.size()
        );

        return json(result);
    }

    private Collection<LoggerConfig> getLoggerConfigs() {
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = loggerContext.getConfiguration();
        return configuration.getLoggers().values();
    }

    @GET
    @Timed
    @Path("/subsystems")
    @ApiOperation(value = "List all logger subsystems and their current levels")
    @Produces(MediaType.APPLICATION_JSON)
    public String subsytems() {
        final Map<String, Object> subsystems = Maps.newHashMap();

        for (Map.Entry<String, Subsystem> subsystem : SUBSYSTEMS.entrySet()) {
            if (!isPermitted(RestPermissions.LOGGERS_READSUBSYSTEM, subsystem.getKey())) {
                continue;
            }
            try {
                final String category = subsystem.getValue().getCategory();
                final Level level = getLoggerLevel(category);

                final Map<String, Object> info = ImmutableMap.<String, Object>of(
                        "title", subsystem.getValue().getTitle(),
                        "category", subsystem.getValue().getCategory(),
                        "description", subsystem.getValue().getDescription(),
                        "level", level.name().toLowerCase(),
                        "level_syslog", SYSLOG_EQUIVALENTS.get(level)
                );

                subsystems.put(subsystem.getKey(), info);
            } catch (Exception e) {
                LOG.error("Error while listing logger subsystem.", e);
            }
        }

        return json(ImmutableMap.of("subsystems", subsystems));
    }

    private Level getLoggerLevel(final String loggerName) {
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = loggerContext.getConfiguration();
        final LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);

        return loggerConfig.getLevel();
    }

    private void setLoggerLevel(final String loggerName, final Level level) {
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration config = context.getConfiguration();

        config.getLoggerConfig(loggerName).setLevel(level);
        context.updateLoggers(config);
    }

    @PUT
    @Timed
    @ApiOperation(value = "Set the loglevel of a whole subsystem",
            notes = "Provided level is falling back to DEBUG if it does not exist")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "No such subsystem.")
    })
    @Path("/subsystems/{subsystem}/level/{level}")
    public Response setSubsystemLoggerLevel(
            @ApiParam(name = "subsystem", required = true) @PathParam("subsystem") String subsystemTitle,
            @ApiParam(name = "level", required = true) @PathParam("level") String level) {
        if (!SUBSYSTEMS.containsKey(subsystemTitle)) {
            LOG.warn("No such subsystem: [{}]. Returning 404.", subsystemTitle);
            return Response.status(404).build();
        }
        checkPermission(RestPermissions.LOGGERS_EDITSUBSYSTEM, subsystemTitle);
        Subsystem subsystem = SUBSYSTEMS.get(subsystemTitle);

        setLoggerLevel(subsystem.getCategory(), Level.toLevel(level.toUpperCase()));

        return Response.ok().build();
    }

    @PUT
    @Timed
    @ApiOperation(value = "Set the loglevel of a single logger",
            notes = "Provided level is falling back to DEBUG if it does not exist")
    @Path("/{loggerName}/level/{level}")
    public Response setSingleLoggerLevel(
            @ApiParam(name = "loggerName", required = true) @PathParam("loggerName") String loggerName,
            @ApiParam(name = "level", required = true) @PathParam("level") String level) {
        checkPermission(RestPermissions.LOGGERS_EDIT, loggerName);

        setLoggerLevel(loggerName, Level.toLevel(level.toUpperCase()));

        return Response.ok().build();
    }

    private static class Subsystem {

        private final String title;
        private final String category;
        private final String description;

        public Subsystem(String title, String category, String description) {
            this.title = title;
            this.category = category;
            this.description = description;
        }

        private String getTitle() {
            return title;
        }

        private String getCategory() {
            return category;
        }

        private String getDescription() {
            return description;
        }
    }
}
