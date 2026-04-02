Ext.define('PBS.window.NotificationThresholds', {
    extend: 'Proxmox.window.Edit',
    xtype: 'pbsNotificationThresholdsEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    subject: gettext('Notification Thresholds'),

    width: 500,
    fieldDefaults: {
        labelWidth: 120,
    },

    items: {
        xtype: 'inputpanel',
        onGetValues: function (values) {
            if (!Ext.isArray(values.delete ?? [])) {
                values.delete = [values.delete];
            }
            for (const k of values.delete ?? []) {
                delete values[k];
            }
            delete values.delete;
            let notificationThresholds = PBS.Utils.printPropertyString(values);
            if (!notificationThresholds) {
                return { delete: 'notification-thresholds' };
            }
            return { 'notification-thresholds': notificationThresholds };
        },
        onSetValues: function (values) {
            if (values['notification-thresholds']) {
                return PBS.Utils.parsePropertyString(values['notification-thresholds']);
            }
        },
        items: [
            {
                xtype: 'box',
                html: `<b>${gettext('S3 requests count:')}</b>`,
                padding: '10 0 5 0',
            },
            {
                xtype: 'proxmoxintegerfield',
                name: 's3-get',
                fieldLabel: gettext('GET'),
                emptyText: gettext('none'),
                fieldStyle: 'text-align: right',
                minValue: 0,
                deleteEmpty: true,
            },
            {
                xtype: 'proxmoxintegerfield',
                name: 's3-put',
                fieldLabel: gettext('PUT'),
                emptyText: gettext('none'),
                fieldStyle: 'text-align: right',
                minValue: 0,
                deleteEmpty: true,
            },
            {
                xtype: 'proxmoxintegerfield',
                name: 's3-post',
                fieldLabel: gettext('POST'),
                emptyText: gettext('none'),
                fieldStyle: 'text-align: right',
                minValue: 0,
                deleteEmpty: true,
            },
            {
                xtype: 'proxmoxintegerfield',
                name: 's3-head',
                fieldLabel: gettext('HEAD'),
                emptyText: gettext('none'),
                fieldStyle: 'text-align: right',
                minValue: 0,
                deleteEmpty: true,
            },
            {
                xtype: 'proxmoxintegerfield',
                name: 's3-delete',
                fieldLabel: gettext('DELETE'),
                emptyText: gettext('none'),
                fieldStyle: 'text-align: right',
                minValue: 0,
                deleteEmpty: true,
            },
            {
                xtype: 'menuseparator',
            },
            {
                xtype: 'box',
                html: `<b>${gettext('S3 traffic volume:')}</b>`,
                padding: '10 0 5 0',
            },
            {
                xtype: 'pmxSizeField',
                name: 's3-upload',
                fieldLabel: gettext('Upload'),
                unit: 'GiB',
                submitAutoScaledSizeUnit: true,
                allowZero: false,
                emptyText: gettext('none'),
            },
            {
                xtype: 'pmxSizeField',
                name: 's3-download',
                fieldLabel: gettext('Download'),
                unit: 'GiB',
                submitAutoScaledSizeUnit: true,
                allowZero: false,
                emptyText: gettext('none'),
            },
        ],
    },
});
