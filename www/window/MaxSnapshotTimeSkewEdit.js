Ext.define('PBS.window.MaxSnapshotTimeSkewEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsMaxSnapshotTimeSkewEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    subject: gettext('Max Snapshot Time Skew'),
    width: 400,

    cbindData: function (initial) {
        let me = this;

        me.datastore = encodeURIComponent(me.datastore);
        me.url = `/api2/extjs/config/datastore/${me.datastore}`;
        me.method = 'PUT';
        me.autoLoad = true;
        return {};
    },

    items: {
        xtype: 'proxmoxtextfield',
        name: 'max-snapshot-time-skew',
        fieldLabel: gettext('Max Snapshot Time Skew'),
        labelWidth: 160,
        emptyText: gettext('Default (1 minute)'),
        deleteEmpty: true,
    },
});
