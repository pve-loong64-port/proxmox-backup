File Formats
============

.. _pxar-format:

Proxmox File Archive Format (``.pxar``)
---------------------------------------

.. graphviz:: pxar-format-overview.dot

.. _pxar-meta-format:

Proxmox File Archive Format - Meta (``.mpxar``)
-----------------------------------------------

Pxar metadata archive with same structure as a regular pxar archive, with the
exception of regular file payloads not being contained within the archive
itself, but rather being stored as payload references to the corresponding pxar
payload (``.ppxar``) file.

Can be used to lookup all the archive entries and metadata without the size
overhead introduced by the file payloads.

.. graphviz:: meta-format-overview.dot

.. _ppxar-format:

Proxmox File Archive Format - Payload (``.ppxar``)
--------------------------------------------------

Pxar payload file storing regular file payloads to be referenced and accessed by
the corresponding pxar metadata (``.mpxar``) archive. Contains a concatenation
of regular file payloads, each prefixed by a `PAYLOAD` header. Further, the
actual referenced payload entries might be separated by padding (full/partial
payloads not referenced), introduced when reusing chunks of a previous backup
run, when chunk boundaries did not aligned to payload entry offsets.

All headers are stored as little-endian.

.. list-table::
   :widths: auto

   * - ``PAYLOAD_START_MARKER``
     - header of ``[u8; 16]`` consisting of type hash and size;
       marks start
   * - ``PAYLOAD``
     - header of ``[u8; 16]`` consisting of type hash and size;
       referenced by metadata archive
   * - Payload
     - raw regular file payload
   * - Padding
     - partial/full unreferenced payloads, caused by unaligned chunk boundary
   * - ...
     - further concatenation of payload header, payload and padding
   * - ``PAYLOAD_TAIL_MARKER``
     - header of ``[u8; 16]`` consisting of type hash and size;
       marks end

.. _pcat1-format:

Proxmox Catalog File Format (``.pcat1``)
----------------------------------------

The catalog file uses the following structure:

.. list-table::
   :widths: auto

   * - ``MAGIC: [u8; 8]``
     - ``[145, 253, 96, 249, 196, 103, 88, 213]``.
   * - ``CATALOG_DATA``
     - Catalog data table. See ``Catalog Data`` :ref:`contents <pcat1-catalog-data>`.
   * - ...
     - More catalog data tables.
   * - ``ROOT_START_OFFSET: u64``
     - Points at the start of the '/' catalog entry. Stored as **little-endian**.

.. _pcat1-catalog-data:

Catalog Data Tables
~~~~~~~~~~~~~~~~~~~

Each table represents the contents of a single directory.

.. list-table::
   :widths: auto

   * - ``TABLE_LENGTH: u64*``
     - Total length of the contents inside this table.
   * - ``ENTRY_COUNT: u64*``
     - Number of entries present in the table.
   * - ``ENTRIES``
     - Concatenated ``ENTRY_COUNT`` ``ENTRY`` structures (see the ``ENTRY`` :ref:`layout <pcat1-entry-layout>` .).

.. _pcat1-entry-layout:

Catalog Table Entries
~~~~~~~~~~~~~~~~~~~~~

Each entry consists of the length and value of its name. Additional, ``file`
and ``directory`` type entries contain a ``PAYLOAD`` field. For directory
entries, the ``offset_back`` payload stores the backward byte distance from the
start of the parent directory table to the start of the referenced child
directory table. The start offset of the child directory table can be computed
as ``parent_table_start - offset_back``.
  
.. list-table::
   :widths: auto

   * - ``ENTRY_TYPE: u8``
     - The type of entry (see entry :ref:`types <pcat1-entry-types>`).
   * - ``NAME_LENGTH: u64*``
     - Length of file or directory name. Stored as variable length encoded u64.
   * - ``NAME: [u8]``
     - The raw UTF-8 string bytes of the name.
   * - ``PAYLOAD``
     - Bytes based on ``ENTRY_TYPE``

       * **Directory ('d')**: ``offset_back: u64*`` (Child-to-Parent offset distance.)
       * **File ('f')**: ``size: u64*`` followed by ``mtime: i64*``

.. _pcat1-entry-types:

Entry Types:
~~~~~~~~~~~~

.. list-table::
   :widths: auto

   * - ``d``
     - Directory
   * - ``f``
     - File
   * - ``l``
     - Symlink
   * - ``h``
     - Hardlink
   * - ``b``
     - Block device
   * - ``c``
     - Char device
   * - ``p``
     - FIFO
   * - ``s``
     - Socket

Variable Length Encoding:
~~~~~~~~~~~~~~~~~~~~~~~~~

The asterisk notation indicates variable-length integers:

* **u64\***: Unsigned variable-length integer. Each encoded byte is 8 bits wide. Bit 7, the most significant bit, is the continuation
  flag. Bits 0 through 6 carry value bits. The first encoded byte stores the lowest order 7 value bits, the second encoded byte
  stores the next 7 value bits, and so on.

* **i64\***: Signed variable-length integer. Each nonzero encoded byte is 8 bits wide. Bit 7, the most significant bit, is the
  continuation flag. Bits 0 through 6 carry value bits. The first encoded byte stores the lowest-order 7 value bits, the second
  encoded byte stores the next 7 value bits, and so on. Non-negative values end with a byte whose continuation flag is clear.
  Negative values end with an additional 0x00 terminator byte.


.. _data-blob-format:

Data Blob Format (``.blob``)
----------------------------

The data blob format is used to store small binary data. The magic number
decides the exact format:

.. list-table::
   :widths: auto

   * - ``[66, 171, 56, 7, 190, 131, 112, 161]``
     - unencrypted
     - uncompressed
   * - ``[49, 185, 88, 66, 111, 182, 163, 127]``
     - unencrypted
     - compressed
   * - ``[123, 103, 133, 190, 34, 45, 76, 240]``
     - encrypted
     - uncompressed
   * - ``[230, 89, 27, 191, 11, 191, 216, 11]``
     - encrypted
     - compressed

The compression algorithm used is ``zstd``. The encryption cipher is
``AES_256_GCM``.

Unencrypted blobs use the following format:

.. list-table::
   :widths: auto

   * - ``MAGIC: [u8; 8]``
   * - ``CRC32: [u8; 4]``
   * - ``Data: (max 16MiB)``

Encrypted blobs additionally contain a 16 byte initialization vector (IV),
followed by a 16 byte authenticated encryption (AE) tag, followed by the
encrypted data:

.. list-table::

   * - ``MAGIC: [u8; 8]``
   * - ``CRC32: [u8; 4]``
   * - ``IV: [u8; 16]``
   * - ``TAG: [u8; 16]``
   * - ``Data: (max 16MiB)``

.. _fixed-index-format:

Fixed Index Format  (``.fidx``)
-------------------------------

All numbers are stored as little-endian.

.. list-table::

   * - ``MAGIC: [u8; 8]``
     - ``[47, 127, 65, 237, 145, 253, 15, 205]``
   * - ``uuid: [u8; 16]``,
     - Unique ID
   * - ``ctime: i64``,
     - Creation Time (epoch)
   * - ``index_csum: [u8; 32]``,
     - SHA-256 over the index (without header) ``SHA256(digest1||digest2||...)``
   * - ``size: u64``,
     - Image size
   * - ``chunk_size: u64``,
     - Chunk size
   * - ``reserved: [u8; 4016]``,
     - Overall header size is one page (4096 bytes)
   * - ``digest1: [u8; 32]``
     - First chunk digest
   * - ``digest2: [u8; 32]``
     - Second chunk digest
   * - ...
     - Next chunk digest ...


.. _dynamic-index-format:

Dynamic Index Format (``.didx``)
--------------------------------

All numbers are stored as little-endian.

.. list-table::

   * - ``MAGIC: [u8; 8]``
     - ``[28, 145, 78, 165, 25, 186, 179, 205]``
   * - ``uuid: [u8; 16]``,
     - Unique ID
   * - ``ctime: i64``,
     - Creation Time (epoch)
   * - ``index_csum: [u8; 32]``,
     - SHA-256 over the index (without header) ``SHA256(offset1||digest1||offset2||digest2||...)``
   * - ``reserved: [u8; 4032]``,
     - Overall header size is one page (4096 bytes)
   * - ``offset1: u64``
     - End of first chunk
   * - ``digest1: [u8; 32]``
     - First chunk digest
   * - ``offset2: u64``
     - End of second chunk
   * - ``digest2: [u8; 32]``
     - Second chunk digest
   * - ...
     - Next chunk offset/digest
