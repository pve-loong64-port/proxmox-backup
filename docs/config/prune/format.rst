Each entry starts with the header ``prune: <name>``, followed by the job
configuration options.

::

  prune: prune-store2
	schedule mon..fri 10:30
	store my-datastore

  prune: ...


You can use the ``proxmox-backup-manager prune-job`` command to manipulate this
file.
