package io.confluent.connect.jdbc.util;

/**
 * Copyright 2019 Wayfair LLC.
 **/

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import metrics_influxdb.UdpInfluxdbProtocol;
import metrics_influxdb.measurements.UdpInlinerSender;

/**
 * Provides a Singleton InfluxDB Client
 *
 * @author Allen Tang
 *
 */
public class InfluxDBClientProvider {

	private static UdpInlinerSender influxdb;

	public static UdpInlinerSender getInstance() {
		if (influxdb == null) {
			initialize();
		}
		return influxdb;
	}

	public static void initialize() {
		final Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("/wayfair/etc/wf-config.ini");
			prop.load(input);

		} catch (final IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (final IOException e) {
					e.printStackTrace();
				}
			}
		}

		final String telegrafHost = prop.getProperty("WF_TELEGRAF_SERVICE").replaceAll("'", "");
		final int telegrafPort = Integer.parseInt(prop.getProperty("WF_TELEGRAF_PORT").replaceAll("'", ""));

		final UdpInfluxdbProtocol protocol = new UdpInfluxdbProtocol(telegrafHost, telegrafPort);
		influxdb = new UdpInlinerSender(protocol);

	}
}