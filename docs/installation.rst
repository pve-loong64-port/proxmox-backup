Installation
============

`Proxmox Backup`_ is split into a server and client part. The server part
can either be installed with a graphical installer or on top of
Debian_ from the provided package repository.

.. include:: system-requirements.rst

.. include:: installation-media.rst

.. _install_pbs:

Server Installation
-------------------

The backup server stores the actual backed up data and provides a web based GUI
for various management tasks such as disk management.

.. note:: You always need a backup server. It is not possible to use
   Proxmox Backup without the server part.

Using our provided disk image (ISO file) is the recommended
installation method, as it includes a convenient installer, a complete
Debian system as well as all necessary packages for the Proxmox Backup
Server.

Once you have created an :ref:`installation_medium`, the booted
:ref:`installer <using_the_installer>` will guide you through the
setup process. It will help you to partition your disks, apply basic
settings such as the language, time zone and network configuration,
and finally install all required packages within minutes.

As an alternative to the interactive installer, advanced users may
wish to install Proxmox Backup Server
:ref:`unattended <install_pbs_unattended>`.

With sufficient Debian knowledge, you can also install Proxmox Backup
Server :ref:`on top of Debian <install_pbs_on_debian>` yourself.

While not recommended, Proxmox Backup Server could also be installed
:ref:`on Proxmox VE <install_pbs_on_pve>`.

.. include:: using-the-installer.rst

.. _install_pbs_unattended:

Install `Proxmox Backup`_ Server Unattended
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It is possible to install Proxmox Backup Server automatically in an
unattended manner. This enables you to fully automate the setup process on
bare-metal. Once the installation is complete and the host has booted up,
automation tools like Ansible can be used to further configure the installation.

The necessary options for the installer must be provided in an answer file.
This file allows the use of filter rules to determine which disks and network
cards should be used.

To use the automated installation, it is first necessary to prepare an
installation ISO.  For more details and information on the unattended
installation see `our wiki
<https://pve.proxmox.com/wiki/Automated_Installation>`_.

.. _install_pbs_on_debian:

Install `Proxmox Backup`_ Server on Debian
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Proxmox ships as a set of Debian packages which can be installed on top of a
standard Debian installation. After configuring the
:ref:`sysadmin_package_repositories`, you need to run:

.. code-block:: console

  # apt update
  # apt install proxmox-backup-server

The above commands keep the current (Debian) kernel and install a minimal
set of required packages.

If you want to install the same set of packages as the installer
does, please use the following:

.. code-block:: console

  # apt update
  # apt install proxmox-backup

This will install all required packages, the Proxmox kernel with ZFS_
support, and a set of common and useful packages.

.. caution:: Installing Proxmox Backup on top of an existing Debian_
  installation looks easy, but it assumes that the base system and local
  storage have been set up correctly. In general this is not trivial, especially
  when LVM_ or ZFS_ is used. The network configuration is completely up to you
  as well.

.. Note:: You can access the web interface of the Proxmox Backup Server with
   your web browser, using HTTPS on port 8007. For example at
   ``https://<ip-or-dns-name>:8007``

.. _install_pbs_on_pve:

Install Proxmox Backup Server on `Proxmox VE`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After configuring the
:ref:`sysadmin_package_repositories`, you need to run:

.. code-block:: console

  # apt update
  # apt install proxmox-backup-server

.. caution:: Installing the backup server directly on the hypervisor
   is not recommended. It is safer to use a separate physical
   server to store backups. Should the hypervisor server fail, you can
   still access the backups.

.. Note:: You can access the web interface of the Proxmox Backup Server with
   your web browser, using HTTPS on port 8007. For example at
   ``https://<ip-or-dns-name>:8007``

.. _install_pbc:

Client Installation
-------------------

Install Proxmox Backup Client on Debian
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Proxmox ships as a set of Debian packages to be installed on top of a standard
Debian installation. After configuring the :ref:`package_repositories_client_only_apt`,
you need to run:

.. code-block:: console

  # apt update
  # apt install proxmox-backup-client

.. note:: The client-only repository should be usable by most recent Debian and
   Ubuntu derivatives.

.. include:: package-repositories.rst
