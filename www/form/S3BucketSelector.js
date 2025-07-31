Ext.define('PBS.form.S3BucketSelector', {
    extend: 'Proxmox.form.ComboGrid',
    alias: 'widget.pbsS3BucketSelector',

    allowBlank: false,
    valueField: 'name',
    displayField: 'name',

    listConfig: {
        width: 350,
        columns: [
            {
                header: gettext('Bucket Name'),
                sortable: true,
                dataIndex: 'name',
                renderer: Ext.String.htmlEncode,
                flex: 1,
            },
        ],
    },

    store: {
        autoLoad: false,
        sorters: 'name',
        fields: ['name'],
        proxy: {
            type: 'proxmox',
            url: `/api2/json/config/s3/${encodeURIComponent(this.endpoint)}/list-buckets`,
        },
    },

    setS3Endpoint: function (endpoint) {
        let me = this;

        if (me.endpoint === endpoint) {
            return;
        }

        me.endpoint = endpoint;
        me.store.removeAll();

        me.setDisabled(false);

        if (!me.firstLoad) {
            me.clearValue();
        }

        me.store.proxy.url = `/api2/json/config/s3/${encodeURIComponent(me.endpoint)}/list-buckets`;
        me.store.load();
        me.firstLoad = false;
    },
});
