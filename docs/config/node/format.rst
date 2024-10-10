The file contains these options:

:acme: The ACME account to use on this node.

:acmedomain0: ACME domain.

:acmedomain1: ACME domain.

:acmedomain2: ACME domain.

:acmedomain3: ACME domain.

:acmedomain4: ACME domain.

:http-proxy: Set proxy for apt and subscription checks.

:email-from: Fallback email from which notifications will be sent.

:ciphers-tls-1.3: List of TLS ciphers for TLS 1.3 that will be used by the proxy. Colon-separated and in descending priority (https://docs.openssl.org/master/man1/openssl-ciphers/). (Proxy has to be restarted for changes to take effect.)

:ciphers-tls-1.2: List of TLS ciphers for TLS <= 1.2 that will be used by the proxy. Colon-separated and in descending priority (https://docs.openssl.org/master/man1/openssl-ciphers/). (Proxy has to be restarted for changes to take effect.)

:default-lang: Default language used in the GUI.

:description: Node description.

:task-log-max-days: Maximum days to keep task logs.

For example:

::

  acme: local
  acmedomain0: first.domain.com
  acmedomain1: second.domain.com
  acmedomain2: third.domain.com
  acmedomain3: fourth.domain.com
  acmedomain4: fifth.domain.com
  http-proxy: internal.proxy.com
  email-from: proxmox@mail.com
  ciphers-tls-1.3: TLS_AES_128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_CHACHA20_POLY1305_SHA256
  ciphers-tls-1.2: RSA_WITH_AES_128_CCM:DHE_RSA_WITH_AES_128_CCM
  default-lang: en
  description: Primary PBS instance
  task-log-max-days: 30


You can use the ``proxmox-backup-manager node`` command to manipulate
this file.
