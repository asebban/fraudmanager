package ma.s2m.fraudmanager.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class PropertiesLoader {

	private final String propertiesFileName;
	private Properties properties = new Properties();

	public PropertiesLoader(String propertiesFileName) {
		this.propertiesFileName = Objects.requireNonNull(propertiesFileName, "fraudmanager.properties");
		this.properties = loadFromClasspath();
	}

	/**
	 * Charge (ou recharge) le fichier .properties depuis le classpath.
	 *
	 * @return les {@link Properties} chargées
	 */
	public Properties loadFromClasspath() {
		if (properties != null) {
			properties.clear();
		}

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = PropertiesLoader.class.getClassLoader();
		}

		try (InputStream in = classLoader.getResourceAsStream(propertiesFileName)) {
			if (in == null) {
				throw new IllegalArgumentException("Properties file not found on classpath: " + propertiesFileName);
			}
			properties.load(in);
			return properties;
		} catch (IOException e) {
			throw new IllegalStateException("Failed to load properties from classpath: " + propertiesFileName, e);
		}
	}

	public Properties getProperties() {
		return properties;
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}

	public String getProperty(String key, String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}

}
