Ext.define('PBS.form.S3ClientSelector', {
    extend: 'Proxmox.form.ComboGrid',
    alias: 'widget.pbsS3ClientSelector',

    allowBlank: false,
    autoSelect: false,
    valueField: 'id',
    displayField: 'id',

    store: {
        model: 'pmx-s3client',
        autoLoad: true,
        sorters: 'id',
    },

    listConfig: {
        columns: [
            {
                header: gettext('S3 Endpoint ID'),
                sortable: true,
                dataIndex: 'id',
                renderer: Ext.String.htmlEncode,
                flex: 1,
            },
            {
                header: gettext('Endpoint'),
                sortable: true,
                dataIndex: 'endpoint',
                flex: 1,
            },
        ],
    },
});
