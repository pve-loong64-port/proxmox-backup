Ext.define('PBS.ShowEncryptionKey', {
    extend: 'Ext.window.Window',
    xtype: 'pbsShowEncryptionKey',
    mixins: ['Proxmox.Mixin.CBind'],

    width: 600,
    modal: true,
    resizable: false,
    title: gettext('Important: Save your Encryption Key'),

    // avoid close by ESC key, force user to more manual action
    onEsc: Ext.emptyFn,
    closable: false,

    items: [
        {
            xtype: 'form',
            layout: {
                type: 'vbox',
                align: 'stretch',
            },
            bodyPadding: 10,
            border: false,
            defaults: {
                anchor: '100%',
                border: false,
                padding: '10 0 0 0',
            },
            items: [
                {
                    xtype: 'textfield',
                    fieldLabel: gettext('Key ID'),
                    labelWidth: 80,
                    inputId: 'keyID',
                    cbind: {
                        value: '{keyID}',
                    },
                    editable: false,
                },
                {
                    xtype: 'textfield',
                    fieldLabel: gettext('Key'),
                    labelWidth: 80,
                    inputId: 'encryption-key',
                    cbind: {
                        value: '{key}',
                    },
                    editable: false,
                },
                {
                    xtype: 'component',
                    html:
                        gettext(
                            'Keep your encryption key safe, but easily accessible for disaster recovery.',
                        ) +
                        '<br>' +
                        gettext('We recommend the following safe-keeping strategy:'),
                },
                {
                    xtype: 'container',
                    layout: 'hbox',
                    items: [
                        {
                            xtype: 'component',
                            html: '1. ' + gettext('Save the key in your password manager.'),
                            flex: 1,
                        },
                        {
                            xtype: 'button',
                            text: gettext('Copy Key'),
                            iconCls: 'fa fa-clipboard x-btn-icon-el-default-toolbar-small',
                            cls: 'x-btn-default-toolbar-small proxmox-inline-button',
                            width: 110,
                            handler: function (b) {
                                document.getElementById('encryption-key').select();
                                document.execCommand('copy');
                            },
                        },
                    ],
                },
                {
                    xtype: 'container',
                    layout: 'hbox',
                    items: [
                        {
                            xtype: 'component',
                            html:
                                '2. ' +
                                gettext(
                                    'Download the key to a USB (pen) drive, placed in secure vault.',
                                ),
                            flex: 1,
                        },
                        {
                            xtype: 'button',
                            text: gettext('Download'),
                            iconCls: 'fa fa-download x-btn-icon-el-default-toolbar-small',
                            cls: 'x-btn-default-toolbar-small proxmox-inline-button',
                            width: 110,
                            handler: function (b) {
                                let showWindow = this.up('window');

                                let filename = `${showWindow.keyID}.enc`;

                                let hiddenElement = document.createElement('a');
                                hiddenElement.href =
                                    'data:attachment/text,' + encodeURI(showWindow.key);
                                hiddenElement.target = '_blank';
                                hiddenElement.download = filename;
                                hiddenElement.click();
                            },
                        },
                    ],
                },
                {
                    xtype: 'container',
                    layout: 'hbox',
                    items: [
                        {
                            xtype: 'component',
                            html:
                                '3. ' +
                                gettext('Print as paperkey, laminated and placed in secure vault.'),
                            flex: 1,
                        },
                        {
                            xtype: 'button',
                            text: gettext('Print Key'),
                            iconCls: 'fa fa-print x-btn-icon-el-default-toolbar-small',
                            cls: 'x-btn-default-toolbar-small proxmox-inline-button',
                            width: 110,
                            handler: function (b) {
                                let showWindow = this.up('window');
                                showWindow.paperkey(showWindow.key);
                            },
                        },
                    ],
                },
            ],
        },
        {
            xtype: 'component',
            border: false,
            padding: '10 10 10 10',
            userCls: 'pmx-hint',
            html: gettext(
                'Please save the encryption key - losing it will render any backup created with it unusable',
            ),
        },
    ],
    buttons: [
        {
            text: gettext('Close'),
            handler: function (b) {
                let showWindow = this.up('window');
                showWindow.close();
            },
        },
    ],
    paperkey: function (keyString) {
        let me = this;

        const key = JSON.parse(keyString);

        const qrwidth = 500;
        let qrdiv = document.createElement('div');
        let qrcode = new QRCode(qrdiv, {
            width: qrwidth,
            height: qrwidth,
            correctLevel: QRCode.CorrectLevel.H,
        });
        qrcode.makeCode(keyString);

        let shortKeyFP = '';
        if (key.fingerprint) {
            shortKeyFP = PBS.Utils.renderKeyID(key.fingerprint);
        }

        let printFrame = document.createElement('iframe');
        Object.assign(printFrame.style, {
            position: 'fixed',
            right: '0',
            bottom: '0',
            width: '0',
            height: '0',
            border: '0',
        });
        const prettifiedKey = JSON.stringify(key, null, 2);
        const keyQrBase64 = qrdiv.children[0].toDataURL('image/png');
        const html = `<html><head><script>
	    window.addEventListener('DOMContentLoaded', (ev) => window.print());
	</script><style>@media print and (max-height: 150mm) {
	  h4, p { margin: 0; font-size: 1em; }
	}</style></head><body style="padding: 5px;">
	<h4>Encryption Key '${me.keyID}' (${shortKeyFP})</h4>
<p style="font-size:1.2em;font-family:monospace;white-space:pre-wrap;overflow-wrap:break-word;">
-----BEGIN PROXMOX BACKUP KEY-----
${prettifiedKey}
-----END PROXMOX BACKUP KEY-----</p>
	<center><img style="width: 100%; max-width: ${qrwidth}px;" src="${keyQrBase64}"></center>
	</body></html>`;

        printFrame.src = 'data:text/html;base64,' + btoa(html);
        document.body.appendChild(printFrame);
        me.on('destroy', () => document.body.removeChild(printFrame));
    },
});

Ext.define('PBS.window.EncryptionKeysEdit', {
    extend: 'Proxmox.window.Edit',
    xtype: 'widget.pbsEncryptionKeysEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    width: 400,

    fieldDefaults: { labelWidth: 120 },

    subject: gettext('Encryption Key'),

    cbindData: function (initialConfig) {
        let me = this;

        me.url = '/api2/extjs/config/encryption-keys';
        me.method = 'POST';
        me.autoLoad = false;

        return {};
    },

    apiCallDone: function (success, response, options) {
        let me = this;

        if (!me.rendered) {
            return;
        }

        let res = response.result.data;
        if (!res) {
            return;
        }

        let keyIdField = me.down('field[name=id]');
        Ext.create('PBS.ShowEncryptionKey', {
            autoShow: true,
            keyID: keyIdField.getValue(),
            key: JSON.stringify(res),
        });
    },

    viewModel: {
        data: {
            keepCryptVisible: false,
        },
    },

    items: [
        {
            xtype: 'pmxDisplayEditField',
            name: 'id',
            fieldLabel: gettext('Encryption Key ID'),
            renderer: Ext.htmlEncode,
            allowBlank: false,
            minLength: 3,
            editable: true,
        },
        {
            xtype: 'displayfield',
            fieldLabel: gettext('Key Source'),
            padding: '2 0',
        },
        {
            xtype: 'radiofield',
            name: 'keysource',
            value: true,
            inputValue: 'new',
            submitValue: false,
            boxLabel: gettext('Auto-generate a new encryption key'),
            padding: '0 0 0 25',
        },
        {
            xtype: 'radiofield',
            name: 'keysource',
            inputValue: 'upload',
            submitValue: false,
            boxLabel: gettext('Upload an existing encryption key'),
            padding: '0 0 0 25',
            listeners: {
                change: function (f, value) {
                    let editWindow = this.up('window');
                    if (!editWindow.rendered) {
                        return;
                    }
                    let uploadKeyField = editWindow.down('field[name=key]');
                    uploadKeyField.setDisabled(!value);
                    uploadKeyField.setHidden(!value);

                    let uploadKeyButton = editWindow.down('filebutton[name=upload-button]');
                    uploadKeyButton.setDisabled(!value);
                    uploadKeyButton.setHidden(!value);

                    if (value) {
                        uploadKeyField.validate();
                    } else {
                        uploadKeyField.reset();
                    }
                },
            },
        },
        {
            xtype: 'fieldcontainer',
            layout: 'hbox',
            items: [
                {
                    xtype: 'proxmoxtextfield',
                    name: 'key',
                    fieldLabel: gettext('Upload From File'),
                    value: '',
                    disabled: true,
                    hidden: true,
                    allowBlank: false,
                    labelAlign: 'right',
                    flex: 1,
                    emptyText: gettext('Drag-and-drop key file here.'),
                    validator: function (value) {
                        if (value.length) {
                            let key;
                            try {
                                key = JSON.parse(value);
                            } catch (e) {
                                return gettext('Failed to parse key - {0}', e);
                            }
                            if (key.data === undefined) {
                                return gettext('Does not seem like a valid Proxmox Backup key!');
                            }
                        }
                        return true;
                    },
                    afterRender: function () {
                        let me = this;
                        if (!window.FileReader) {
                            // No FileReader support in this browser
                            return;
                        }
                        let cancel = function (ev) {
                            ev = ev.event;
                            if (ev.preventDefault) {
                                ev.preventDefault();
                            }
                        };
                        this.inputEl.on('dragover', cancel);
                        this.inputEl.on('dragenter', cancel);
                        this.inputEl.on('drop', (ev) => {
                            cancel(ev);
                            let reader = new FileReader();
                            reader.onload = (ev) => me.setValue(ev.target.result);
                            reader.readAsText(ev.event.dataTransfer.files[0]);
                        });
                    },
                },
                {
                    xtype: 'filebutton',
                    name: 'upload-button',
                    iconCls: 'fa fa-fw fa-folder-open-o x-btn-icon-el-default-toolbar-small',
                    cls: 'x-btn-default-toolbar-small proxmox-inline-button',
                    margin: '0 0 0 4',
                    disabled: true,
                    hidden: true,
                    listeners: {
                        change: function (btn, e, value) {
                            let ev = e.event;
                            let field = btn.up().down('proxmoxtextfield[name=key]');
                            let reader = new FileReader();
                            reader.onload = (ev) => field.setValue(ev.target.result);
                            reader.readAsText(ev.target.files[0]);
                            btn.reset();
                        },
                    },
                },
            ],
        },
    ],
});
