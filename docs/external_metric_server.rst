.. _external_metric_server:

External Metric Server
----------------------

Proxmox Backup Server periodically sends various metrics about your host's memory,
network and disk activity to configured external metric servers.

Currently supported are:

 * InfluxDB (HTTP) (see https://docs.influxdata.com/influxdb/v2/ )
 * InfluxDB (UDP) (see https://docs.influxdata.com/influxdb/v1/ )

The external metric server definitions are saved in
'/etc/proxmox-backup/metricserver.cfg', and can be edited through the web
interface.

.. note::

   Using HTTP is recommended as UDP support has been dropped in InfluxDB v2.

InfluxDB (HTTP) plugin configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The plugin can be configured to use the HTTP(s) API of InfluxDB 2.x.
InfluxDB 1.8.x does contain a forwards compatible API endpoint for this v2 API.

Since InfluxDB's v2 API is only available with authentication, you have
to generate a token that can write into the correct bucket and set it.

In the v2 compatible API of 1.8.x, you can use 'user:password' as token
(if required), and can omit the 'organization' since that has no meaning in InfluxDB 1.x.

You can also set the maximum batch size (default 25000000 bytes) with the
'max-body-size' setting (this corresponds to the InfluxDB setting with the
same name).

InfluxDB (UDP) plugin configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Proxmox Backup Server can also send data via UDP. This requires the InfluxDB
server to be configured correctly. The MTU can also be configured here if
necessary.

Here is an example configuration for InfluxDB (on your InfluxDB server):

----
[[udp]]
   enabled = true
   bind-address = "0.0.0.0:8089"
   database = "proxmox"
   batch-size = 1000
   batch-timeout = "1s"
----

With this configuration, the InfluxDB server listens on all IP addresses on
port 8089, and writes the data in the *proxmox* database.
