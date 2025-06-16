Ext.define('PBS.DataStoreEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsDataStoreEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    subject: gettext('Datastore'),
    isAdd: true,

    bodyPadding: 0,
    showProgress: true,

    cbindData: function (initialConfig) {
        var me = this;

        let name = initialConfig.name;
        let baseurl = '/api2/extjs/config/datastore';

        me.isCreate = !name;
        if (!me.isCreate) {
            me.defaultFocus = 'textfield[name=comment]';
        }
        me.url = name ? baseurl + '/' + name : baseurl;
        me.method = name ? 'PUT' : 'POST';
        me.scheduleValue = name ? null : 'daily';
        me.autoLoad = !!name;
        return {};
    },

    items: {
        xtype: 'tabpanel',
        bodyPadding: 10,
        listeners: {
            tabchange: function (tb, newCard) {
                Ext.GlobalEvents.fireEvent('proxmoxShowHelp', newCard.onlineHelp);
            },
        },
        items: [
            {
                title: gettext('General'),
                xtype: 'inputpanel',
                onlineHelp: 'datastore_intro',
                cbind: {
                    isCreate: '{isCreate}',
                },
                column1: [
                    {
                        xtype: 'pmxDisplayEditField',
                        cbind: {
                            editable: '{isCreate}',
                        },
                        name: 'name',
                        allowBlank: false,
                        fieldLabel: gettext('Name'),
                    },
                    {
                        xtype: 'proxmoxKVComboBox',
                        name: 'datastore-type',
                        fieldLabel: gettext('Datastore Type'),
                        value: '__default__',
                        submitValue: false,
                        comboItems: [
                            ['__default__', gettext('Local')],
                            ['removable', gettext('Removable')],
                        ],
                        cbind: {
                            disabled: '{!isCreate}',
                        },
                        listeners: {
                            change: function (checkbox, selected) {
                                let isRemovable = selected === 'removable';

                                let inputPanel = checkbox.up('inputpanel');
                                let pathField = inputPanel.down('[name=path]');
                                let uuidEditField = inputPanel.down('[name=backing-device]');

                                uuidEditField.setDisabled(!isRemovable);
                                uuidEditField.allowBlank = !isRemovable;
                                uuidEditField.setValue('');

                                if (isRemovable) {
                                    pathField.setFieldLabel(gettext('Path on Device'));
                                    pathField.setEmptyText(gettext('A relative path'));
                                } else {
                                    pathField.setFieldLabel(gettext('Backing Path'));
                                    pathField.setEmptyText(gettext('An absolute path'));
                                }
                            },
                        },
                    },
                    {
                        xtype: 'pmxDisplayEditField',
                        cbind: {
                            editable: '{isCreate}',
                        },
                        name: 'path',
                        allowBlank: false,
                        fieldLabel: gettext('Backing Path'),
                        emptyText: gettext('An absolute path'),
                        validator: (val) => val?.trim() !== '/',
                    },
                ],
                column2: [
                    {
                        xtype: 'pbsCalendarEvent',
                        name: 'gc-schedule',
                        fieldLabel: gettext('GC Schedule'),
                        emptyText: gettext('none'),
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                            value: '{scheduleValue}',
                        },
                    },
                    {
                        xtype: 'pbsCalendarEvent',
                        name: 'prune-schedule',
                        fieldLabel: gettext('Prune Schedule'),
                        value: 'daily',
                        emptyText: gettext('none'),
                        cbind: {
                            deleteEmpty: '{!isCreate}',
                            value: '{scheduleValue}',
                        },
                    },
                    {
                        xtype: 'pbsPartitionSelector',
                        fieldLabel: gettext('Device'),
                        name: 'backing-device',
                        disabled: true,
                        allowBlank: true,
                        cbind: {
                            editable: '{isCreate}',
                        },
                        emptyText: gettext('Device path'),
                    },
                ],
                columnB: [
                    {
                        xtype: 'textfield',
                        name: 'comment',
                        fieldLabel: gettext('Comment'),
                    },
                ],
                advancedColumn1: [
                    {
                        xtype: 'checkbox',
                        name: 'reuse-datastore',
                        fieldLabel: gettext('Reuse existing datastore'),
                    },
                ],

                onGetValues: function (values) {
                    let me = this;

                    if (me.isCreate) {
                        // New datastores default to using the notification system
                        values['notification-mode'] = 'notification-system';
                    }
                    return values;
                },
            },
            {
                title: gettext('Prune Options'),
                xtype: 'pbsPruneInputPanel',
                cbind: {
                    isCreate: '{isCreate}',
                },
                onlineHelp: 'backup_pruning',
            },
        ],
    },
});
