Ext.define('pmx-s3client', {
    extend: 'Ext.data.Model',
    fields: ['id', 'endpoint', 'port', 'access-key', 'secret-key', 'region', 'fingerprint'],
    idProperty: 'id',
    proxy: {
        type: 'proxmox',
        url: '/api2/json/config/s3',
    },
});

Ext.define('PBS.config.S3ClientView', {
    extend: 'Ext.grid.GridPanel',
    alias: 'widget.pbsS3ClientView',

    title: gettext('S3 Endpoints'),

    stateful: true,
    stateId: 'grid-s3clients',
    tools: [PBS.Utils.get_help_tool('backup-s3-client')],

    controller: {
        xclass: 'Ext.app.ViewController',

        addS3Client: function () {
            let me = this;
            Ext.create('PBS.window.S3ClientEdit', {
                listeners: {
                    destroy: function () {
                        me.reload();
                    },
                },
            }).show();
        },

        editS3Client: function () {
            let me = this;
            let view = me.getView();
            let selection = view.getSelection();
            if (selection.length < 1) {
                return;
            }

            Ext.create('PBS.window.S3ClientEdit', {
                id: selection[0].data.id,
                listeners: {
                    destroy: function () {
                        me.reload();
                    },
                },
            }).show();
        },

        reload: function () {
            this.getView().getStore().rstore.load();
        },

        init: function (view) {
            Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
        },
    },

    listeners: {
        activate: 'reload',
        itemdblclick: 'editS3Client',
    },

    store: {
        type: 'diff',
        autoDestroy: true,
        autoDestroyRstore: true,
        sorters: 'id',
        rstore: {
            type: 'update',
            storeid: 'pmx-s3client',
            model: 'pmx-s3client',
            autoStart: true,
            interval: 5000,
        },
    },

    tbar: [
        {
            xtype: 'proxmoxButton',
            text: gettext('Add'),
            handler: 'addS3Client',
            selModel: false,
        },
        {
            xtype: 'proxmoxButton',
            text: gettext('Edit'),
            handler: 'editS3Client',
            disabled: true,
        },
        {
            xtype: 'proxmoxStdRemoveButton',
            baseurl: '/config/s3',
            callback: 'reload',
        },
    ],

    viewConfig: {
        trackOver: false,
    },

    columns: [
        {
            dataIndex: 'id',
            header: gettext('S3 Client ID'),
            renderer: Ext.String.htmlEncode,
            sortable: true,
            width: 200,
        },
        {
            dataIndex: 'endpoint',
            header: gettext('Endpoint'),
            sortable: true,
            width: 200,
        },
        {
            dataIndex: 'port',
            header: gettext('Port'),
            renderer: Ext.String.htmlEncode,
            sortable: true,
            width: 100,
        },
        {
            dataIndex: 'region',
            header: gettext('Region'),
            renderer: Ext.String.htmlEncode,
            sortable: true,
            width: 100,
        },
        {
            dataIndex: 'fingerprint',
            header: gettext('Fingerprint'),
            renderer: Ext.String.htmlEncode,
            sortable: false,
            flex: 1,
        },
    ],
});
