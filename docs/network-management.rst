.. _sysadmin_network_configuration:

Network Management
==================

.. image:: images/screenshots/pbs-gui-system-config.png
  :target: _images/pbs-gui-system-config.png
  :align: right
  :alt: System and Network Configuration Overview

`Proxmox Backup`_ Server provides both a web interface and a command-line tool
for network configuration. You can find the configuration options in the web
interface under the **Network Interfaces** section of the **Configuration** menu
tree item. The command-line tool is accessed via the ``network`` subcommand.
These interfaces allow you to carry out some basic network management tasks,
such as adding, configuring, and removing network interfaces.

.. note:: Any changes made to the network configuration are not
  applied, until you click on **Apply Configuration** or enter the ``network
  reload`` command. This allows you to make many changes at once. It also allows
  you to ensure that your changes are correct before applying them, as making a
  mistake here can render the server inaccessible over the network.

To get a list of available interfaces, use the following command:

.. code-block:: console

  # proxmox-backup-manager network list
  ┌───────┬────────┬───────────┬────────┬─────────────┬──────────────┬──────────────┐
  │ name  │ type   │ autostart │ method │ address     │ gateway      │ ports/slaves │
  ╞═══════╪════════╪═══════════╪════════╪═════════════╪══════════════╪══════════════╡
  │ bond0 │ bond   │         1 │ static │ x.x.x.x/x   │ x.x.x.x      │ ens18 ens19  │
  ├───────┼────────┼───────────┼────────┼─────────────┼──────────────┼──────────────┤
  │ ens18 │ eth    │         1 │ manual │             │              │              │
  ├───────┼────────┼───────────┼────────┼─────────────┼──────────────┼──────────────┤
  │ ens19 │ eth    │         1 │ manual │             │              │              │
  └───────┴────────┴───────────┴────────┴─────────────┴──────────────┴──────────────┘

To add a new network interface, use the ``create`` subcommand with the relevant
parameters. For example, you may want to set up a bond, for the purpose of
network redundancy. The following command shows a template for creating the bond shown
in the list above:

.. code-block:: console

  # proxmox-backup-manager network create bond0 --type bond --bond_mode active-backup --slaves ens18,ens19 --autostart true --cidr x.x.x.x/x --gateway x.x.x.x

.. image:: images/screenshots/pbs-gui-network-create-bond.png
  :target: _images/pbs-gui-network-create-bond.png
  :align: right
  :alt: Add a network interface

You can make changes to the configuration of a network interface with the
``update`` subcommand:

.. code-block:: console

  # proxmox-backup-manager network update bond0 --cidr y.y.y.y/y

You can also remove a network interface:

.. code-block:: console

   # proxmox-backup-manager network remove bond0

The pending changes for the network configuration file will appear at the bottom of the
web interface. You can also view these changes, by using the command:

.. code-block:: console

  # proxmox-backup-manager network changes

If you would like to cancel all changes at this point, you can either click on
the **Revert** button or use the following command:

.. code-block:: console

  # proxmox-backup-manager network revert

If you are happy with the changes and would like to write them into the
configuration file, select **Apply Configuration**. The corresponding command
is:

.. code-block:: console

  # proxmox-backup-manager network reload

.. note:: This command and corresponding GUI button rely on the ``ifreload``
  command, from the package ``ifupdown2``. This package is included within the
  Proxmox Backup Server installation, however, you may have to install it yourself,
  if you have installed Proxmox Backup Server on top of Debian or a Proxmox VE
  version prior to version 7.

You can also configure DNS settings, from the **DNS** section
of **Configuration** or by using the ``dns`` subcommand of
``proxmox-backup-manager``.


.. include:: traffic-control.rst


Overriding network device names
-------------------------------

When upgrading kernels, adding PCIe devices or updating your BIOS, automatically
generated network interface names can change. To alleviate this issues, Proxmox
Backup Server provides a tool for automatically generating .link files for
overriding the name of network devices. It also automatically replaces the
occurences of the old interface name in `/etc/network/interfaces`.

The generated link files are stored in `/usr/local/lib/systemd/network`. For the
interfaces file a new file will be generated in the same place with a `.new`
suffix. This way you can inspect the changes made to the configuration by
using diff (or another diff viewer of your choice):

----
diff -y /etc/network/interfaces /etc/network/interfaces.new
----

If you see any problematic changes or want to revert the changes made by the
pinning tool **before rebooting**, simply delete all `.new` files and the
respective link files from `/usr/local/lib/systemd/network`.

The following command will generate a .link file for all physical network
interfaces that do not yet have a .link file and update selected Proxmox VE
configuration files (see above). The generated names will use the default prefix
`nic`, so the resulting interface names will be `nic1`, `nic2`, ...

----
proxmox-network-interface-pinning generate
----

You can override the default prefix with the `--prefix` flag:

----
proxmox-network-interface-pinning generate --prefix myprefix
----

It is also possible to pin only a specific interface:

----
proxmox-network-interface-pinning generate --interface enp1s0
----

When pinning a specific interface, you can specify the exact name that the
interface should be pinned to:

----
proxmox-network-interface-pinning generate --interface enp1s0 --target-name if42
----

In order to apply the changes made by `proxmox-network-interface-pinning` to the
network configuration, the host needs to be rebooted.

