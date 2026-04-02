Ext.define('PBS.window.CounterResetScheduleEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsCounterResetScheduleEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    userid: undefined,
    isAdd: false,

    subject: gettext('Counter Reset Schedule'),

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
        fieldLabel: gettext('Counter Reset Schedule'),
        emptyText: gettext('none (disabled)'),
    },
});
