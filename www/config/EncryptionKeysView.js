Ext.define('pbs-encryption-keys', {
    extend: 'Ext.data.Model',
    fields: ['id', 'type', 'hint', 'fingerprint', 'created', 'archived-at'],
    idProperty: 'id',
});

Ext.define('PBS.config.EncryptionKeysView', {
    extend: 'Ext.grid.GridPanel',
    alias: 'widget.pbsEncryptionKeysView',

    title: gettext('Encryption Keys'),

    stateful: true,
    stateId: 'grid-encryption-keys',

    controller: {
        xclass: 'Ext.app.ViewController',

        addSyncEncryptionKey: function () {
            let me = this;
            Ext.create('PBS.window.EncryptionKeysEdit', {
                listeners: {
                    destroy: function () {
                        me.reload();
                    },
                },
            }).show();
        },

        addTapeEncryptionKey: function () {
            let me = this;
            Ext.create('PBS.TapeManagement.EncryptionEditWindow', {
                listeners: {
                    destroy: function () {
                        me.reload();
                    },
                },
            }).show();
        },

        archiveEncryptionKey: function () {
            let me = this;
            let view = me.getView();
            let selection = view.getSelection();

            if (!selection || selection.length < 1) {
                return;
            }

            if (selection[0].data.type === 'tape') {
                Ext.Msg.alert(gettext('Error'), gettext('cannot archive tape key'));
                return;
            }

            let keyID = selection[0].data.id;
            Proxmox.Utils.API2Request({
                url: `/api2/extjs/config/encryption-keys/${keyID}`,
                method: 'POST',
                waitMsgTarget: view,
                failure: function (response, opts) {
                    Ext.Msg.alert(gettext('Error'), response.htmlStatus);
                },
                success: function (response, opts) {
                    view.getSelectionModel().deselectAll();
                    me.reload();
                },
            });
        },

        removeEncryptionKey: function () {
            let me = this;
            let view = me.getView();
            let selection = view.getSelection();

            if (!selection || selection.length < 1) {
                return;
            }

            let keyType = selection[0].data.type;
            let keyID = selection[0].data.id;
            let keyFp = selection[0].data.fingerprint;
            let endpointUrl =
                keyType === 'tape'
                    ? `/api2/extjs/config/tape-encryption-keys/${keyFp}`
                    : `/api2/extjs/config/encryption-keys/${keyID}`;

            Ext.create('Proxmox.window.SafeDestroy', {
                url: endpointUrl,
                item: {
                    id: `${keyType}/${keyID}`,
                },
                autoShow: true,
                showProgress: false,
                taskName: 'remove-encryption-key',
                listeners: {
                    destroy: () => me.reload(),
                },
                additionalItems: [
                    {
                        xtype: 'box',
                        userCls: 'pmx-hint',
                        style: {
                            'inline-size': '375px',
                            'overflow-wrap': 'break-word',
                        },
                        padding: '5',
                        html: gettext(
                            'Make sure you have a backup of the encryption key!<br><br>You will not be able to decrypt contents encrypted with this key once removed.',
                        ),
                    },
                ],
            }).show();
        },

        restoreEncryptionKey: function () {
            Ext.create('Proxmox.window.Edit', {
                title: gettext('Restore Key'),
                isCreate: true,
                submitText: gettext('Restore'),
                method: 'POST',
                url: `/api2/extjs/tape/drive`,
                submitUrl: function (url, values) {
                    let drive = values.drive;
                    delete values.drive;
                    return `${url}/${drive}/restore-key`;
                },
                items: [
                    {
                        xtype: 'pbsDriveSelector',
                        fieldLabel: gettext('Drive'),
                        name: 'drive',
                    },
                    {
                        xtype: 'textfield',
                        inputType: 'password',
                        fieldLabel: gettext('Password'),
                        name: 'password',
                    },
                ],
            }).show();
        },

        reload: async function () {
            let me = this;
            let view = me.getView();

            let syncKeysFuture = Proxmox.Async.api2({
                url: '/api2/extjs/config/encryption-keys',
                method: 'GET',
                params: {
                    'include-archived': true,
                },
            });

            let tapeKeysFuture = Proxmox.Async.api2({
                url: '/api2/extjs/config/tape-encryption-keys',
                method: 'GET',
            });

            let combinedKeys = [];

            try {
                let syncKeys = await syncKeysFuture;
                if (syncKeys?.result?.data) {
                    syncKeys.result.data.forEach((key) => {
                        key.type = 'sync';
                        combinedKeys.push(key);
                    });
                }
            } catch (error) {
                Ext.Msg.alert(gettext('Error'), error);
            }

            try {
                let tapeKeys = await tapeKeysFuture;
                if (tapeKeys?.result?.data) {
                    tapeKeys.result.data.forEach((key) => {
                        key.id = `${key.created}-${key.fingerprint.substring(0, 9).replace(/:/g, '')}`;
                        key.type = 'tape';
                        combinedKeys.push(key);
                    });
                }
            } catch (error) {
                Ext.Msg.alert(gettext('Error'), error);
            }

            let store = view.getStore().rstore;
            store.loadData(combinedKeys);
            store.fireEvent('load', store, combinedKeys, true);
        },

        init: function () {
            let me = this;
            me.reload();
            me.updateTask = Ext.TaskManager.start({
                run: () => me.reload(),
                interval: 5000,
            });
        },

        destroy: function () {
            let me = this;
            if (me.updateTask) {
                Ext.TaskManager.stop(me.updateTask);
            }
        },
    },

    listeners: {
        activate: 'reload',
    },

    store: {
        type: 'diff',
        autoDestroy: true,
        autoDestroyRstore: true,
        sorters: 'id',
        rstore: {
            type: 'store',
            storeid: 'pbs-encryption-keys',
            model: 'pbs-encryption-keys',
            proxy: {
                type: 'memory',
            },
        },
    },

    tbar: [
        {
            text: gettext('Add'),
            menu: [
                {
                    text: gettext('Add Sync Encryption Key'),
                    iconCls: 'fa fa-refresh',
                    handler: 'addSyncEncryptionKey',
                    selModel: false,
                },
                {
                    text: gettext('Add Tape Encryption Key'),
                    iconCls: 'pbs-icon-tape',
                    handler: 'addTapeEncryptionKey',
                    selModel: false,
                },
            ],
        },
        '-',
        {
            xtype: 'proxmoxButton',
            text: gettext('Archive'),
            handler: 'archiveEncryptionKey',
            dangerous: true,
            confirmMsg: Ext.String.format(
                gettext('Archiving will render the key unusable to encrypt new content, proceed?'),
            ),
            disabled: true,
            enableFn: (item) => item.data.type === 'sync' && !item.data['archived-at'],
        },
        '-',
        {
            xtype: 'proxmoxButton',
            text: gettext('Remove'),
            handler: 'removeEncryptionKey',
            disabled: true,
            enableFn: (item) =>
                (item.data.type === 'sync' && !!item.data['archived-at']) ||
                item.data.type === 'tape',
        },
        '-',
        {
            text: gettext('Restore Key'),
            xtype: 'proxmoxButton',
            handler: 'restoreEncryptionKey',
            disabled: true,
            enableFn: (item) => item.data.type === 'tape',
        },
    ],

    viewConfig: {
        trackOver: false,
    },

    columns: [
        {
            dataIndex: 'id',
            header: gettext('Key ID'),
            renderer: Ext.String.htmlEncode,
            sortable: true,
            width: 200,
        },
        {
            dataIndex: 'type',
            header: gettext('Type'),
            renderer: function (value) {
                let iconCls, label;
                if (value === 'sync') {
                    iconCls = 'fa fa-refresh';
                    label = gettext('Sync');
                } else if (value === 'tape') {
                    iconCls = 'fa pbs-icon-tape';
                    label = gettext('Tape');
                } else {
                    return value;
                }
                return `<i class="${iconCls}"></i> ${label}`;
            },
            sortable: true,
            width: 50,
        },
        {
            dataIndex: 'hint',
            header: gettext('Hint'),
            sortable: true,
            width: 100,
        },
        {
            dataIndex: 'fingerprint',
            header: gettext('Fingerprint'),
            sortable: false,
            width: 600,
        },
        {
            dataIndex: 'created',
            header: gettext('Created'),
            renderer: Proxmox.Utils.render_timestamp,
            sortable: true,
            flex: 1,
        },
        {
            dataIndex: 'archived-at',
            header: gettext('Archived'),
            renderer: (val) => (val ? Proxmox.Utils.render_timestamp(val) : ''),
            sortable: true,
            flex: 1,
        },
    ],
});
