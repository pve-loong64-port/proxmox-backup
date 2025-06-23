Ext.define('PBS.TapeManagement.BackupJobEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsTapeBackupJobEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    userid: undefined,

    isAdd: true,

    subject: gettext('Tape Backup Job'),

    fieldDefaults: { labelWidth: 120 },

    bodyPadding: 0,

    onlineHelp: 'tape_backup_job_config',

    cbindData: function (initialConfig) {
        let me = this;

        let baseurl = '/api2/extjs/config/tape-backup-job';
        let id = initialConfig.id;

        me.isCreate = !id;
        me.url = id ? `${baseurl}/${id}` : baseurl;
        me.method = id ? 'PUT' : 'POST';
        me.autoLoad = !!id;
        me.scheduleValue = id ? null : 'daily';
        me.authid = id ? null : Proxmox.UserName;
        me.editDatastore = me.datastore === undefined && me.isCreate;
        return {};
    },

    controller: {
        xclass: 'Ext.app.ViewController',
        control: {
            'pbsDataStoreSelector[name=store]': {
                change: 'storeChange',
            },
        },

        storeChange: function (field, value) {
            let me = this;
            let nsSelector = me.lookup('namespace');
            nsSelector.setDatastore(value);
        },
    },

    items: {
        xtype: 'tabpanel',
        bodyPadding: 10,
        border: 0,
        items: [
            {
                title: gettext('Options'),
                xtype: 'inputpanel',
                onGetValues: function (values) {
                    let me = this;

                    if (values['export-media-set'] && !me.up('pbsTapeBackupJobEdit').isCreate) {
                        Proxmox.Utils.assemble_field_data(values, { delete: 'eject-media' });
                    }
                    PBS.Utils.delete_if_default(values, 'notify-user');

                    if (me.isCreate) {
                        delete values.delete;
                    }

                    return values;
                },
                cbind: {
                    isCreate: '{isCreate}', // pass it through
                },
                column1: [
                    {
                        xtype: 'pmxDisplayEditField',
                        name: 'id',
                        fieldLabel: gettext('Job ID'),
                        renderer: Ext.htmlEncode,
                        allowBlank: false,
                        cbind: {
                            editable: '{isCreate}',
                        },
                    },
                    {
                        xtype: 'pbsDataStoreSelector',
                        fieldLabel: gettext('Local Datastore'),
                        name: 'store',
                        listeners: {
                            change: function (field, value) {
                                let me = this;
                                me.up('tabpanel').down('pbsGroupFilter').setLocalDatastore(value);
                            },
                        },
                    },
                    {
                        xtype: 'pbsNamespaceSelector',
                        fieldLabel: gettext('Local Namespace'),
                        reference: 'namespace',
                        name: 'ns',
                    },
                    {
                        xtype: 'pbsMediaPoolSelector',
                        fieldLabel: gettext('Media Pool'),
                        name: 'pool',
                    },
                    {
                        xtype: 'pbsDriveSelector',
                        fieldLabel: gettext('Drive'),
                        name: 'drive',
                    },
                ],

                column2: [
                    {
                        fieldLabel: gettext('Schedule'),
                        xtype: 'pbsCalendarEvent',
                        name: 'schedule',
                        emptyText: gettext('none (disabled)'),
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                            value: '{scheduleValue}',
                        },
                    },
                    {
                        fieldLabel: gettext('Export Media-Set'),
                        xtype: 'proxmoxcheckbox',
                        name: 'export-media-set',
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                        },
                        listeners: {
                            change: function (cb, value) {
                                let me = this;
                                let eject = me
                                    .up('window')
                                    .down('proxmoxcheckbox[name=eject-media]');
                                if (value) {
                                    eject.setValue(false);
                                }
                                eject.setDisabled(!!value);
                            },
                        },
                    },
                    {
                        fieldLabel: gettext('Eject Media'),
                        xtype: 'proxmoxcheckbox',
                        name: 'eject-media',
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                        },
                    },
                    {
                        fieldLabel: gettext('Latest Only'),
                        xtype: 'proxmoxcheckbox',
                        name: 'latest-only',
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                        },
                    },
                    {
                        xtype: 'pbsNamespaceMaxDepth',
                        name: 'max-depth',
                        fieldLabel: gettext('Max. Depth'),
                        deleteEmpty: true,
                    },
                ],

                columnB: [
                    {
                        fieldLabel: gettext('Comment'),
                        xtype: 'proxmoxtextfield',
                        name: 'comment',
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                        },
                    },
                ],

                advancedColumn1: [
                    {
                        xtype: 'proxmoxintegerfield',
                        name: 'worker-threads',
                        fieldLabel: gettext('# of Worker Threads'),
                        emptyText: '1',
                        minValue: 1,
                        maxValue: 32,
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                        },
                    },
                ],
            },
            {
                xtype: 'inputpanel',
                onGetValues: function (values) {
                    PBS.Utils.delete_if_default(values, 'group-filter');
                    if (
                        Ext.isArray(values['group-filter']) &&
                        values['group-filter'].length === 0
                    ) {
                        delete values['group-filter'];
                        values.delete = 'group-filter';
                    }
                    if (this.isCreate) {
                        delete values.delete;
                    }
                    return values;
                },
                cbind: {
                    isCreate: '{isCreate}', // pass it through
                },
                title: gettext('Group Filter'),
                items: [
                    {
                        xtype: 'pbsGroupFilter',
                        name: 'group-filter',
                    },
                ],
            },
            {
                xtype: 'inputpanel',
                title: gettext('Notifications'),

                viewModel: {
                    data: {
                        notificationMode: undefined,
                    },
                    formulas: {
                        notificationSystemSelected: (get) =>
                            get('notificationMode')['notification-mode'] === 'notification-system',
                    },
                },

                onSetValues: function (values) {
                    let me = this;

                    let mode = values['notification-mode'];
                    me.getViewModel().set('notificationMode', { 'notification-mode': mode });

                    return values;
                },

                items: [
                    {
                        xtype: 'radiogroup',
                        height: '15px',
                        layout: {
                            type: 'vbox',
                        },
                        bind: {
                            value: '{notificationMode}',
                        },
                        items: [
                            {
                                xtype: 'radiofield',
                                name: 'notification-mode',
                                inputValue: 'notification-system',
                                boxLabel: gettext('Use global notification settings'),
                                cbind: {
                                    checked: '{isCreate}',
                                },
                            },
                            {
                                xtype: 'radiofield',
                                name: 'notification-mode',
                                inputValue: 'legacy-sendmail',
                                boxLabel: gettext('Use sendmail to send an email (legacy)'),
                            },
                        ],
                    },
                    {
                        xtype: 'pmxUserSelector',
                        name: 'notify-user',
                        fieldLabel: gettext('Recipient'),
                        emptyText: 'root@pam',
                        allowBlank: false,
                        value: null,
                        renderer: Ext.String.htmlEncode,
                        padding: '0 0 0 50',
                        disabled: true,
                        bind: {
                            disabled: '{notificationSystemSelected}',
                        },
                    },
                ],
            },
        ],
    },
});
