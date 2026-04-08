Ext.define('PBS.window.ThresholdResetScheduleEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsThresholdResetScheduleEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    userid: undefined,
    isAdd: false,

    subject: gettext('Threshold Reset Schedule'),

    cbindData: function (initial) {
        let me = this;

        me.datastore = encodeURIComponent(me.datastore);
        me.url = `/api2/extjs/config/datastore/${me.datastore}`;
        me.method = 'PUT';
        me.autoLoad = true;
        return {};
    },

    items: {
        xtype: 'pbsCalendarEvent',
        name: 'counter-reset-schedule',
        fieldLabel: gettext('Threshold Reset Schedule'),
        emptyText: gettext('none (disabled)'),
    },
});
