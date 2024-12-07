package org.apache.kafka.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

import java.util.function.Function;

public class PluginVersionUtils {

    public static final String UNDEFINED_VERSION = "undefined";

    public static VersionRange connectorVersionRequirement(String version) throws InvalidVersionSpecificationException {
        if (version == null || version.equals("latest")) {
            return null;
        }
        version = version.trim();

        // check first if the given version is valid
        VersionRange range = VersionRange.createFromVersionSpec(version);

        if (range.hasRestrictions()) {
            return range;
        }
        // now if the version is not enclosed we consider it as a hard requirement and enclose it in []
        version = "[" + version + "]";
        return VersionRange.createFromVersionSpec(version);
    }

    public static class PluginVersionValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {

            try {
                connectorVersionRequirement((String) value);
            } catch (InvalidVersionSpecificationException e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }

    }

    public static <T> String getVersionOrUndefined(T obj, Function<ClassLoader, LoaderSwap> pluginLoaderSwapper) {
        if (obj == null) {
            return UNDEFINED_VERSION;
        }
        try (LoaderSwap swap = pluginLoaderSwapper.apply(obj.getClass().getClassLoader())) {
            if (obj instanceof Versioned) {
                return ((Versioned) obj).version();
            }
        }
        return UNDEFINED_VERSION;
    }
}



