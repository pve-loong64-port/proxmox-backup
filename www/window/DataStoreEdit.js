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
                            ['s3', gettext('S3 (experimental)')],
                        ],
                        cbind: {
                            disabled: '{!isCreate}',
                        },
                        listeners: {
                            change: function (checkbox, selected) {
                                let isRemovable = selected === 'removable';
                                let isS3 = selected === 's3';

                                let inputPanel = checkbox.up('inputpanel');
                                let pathField = inputPanel.down('[name=path]');
                                let uuidEditField = inputPanel.down('[name=backing-device]');
                                let bucketField = inputPanel.down('[name=bucket]');
                                let s3ClientSelector = inputPanel.down('[name=s3client]');
                                let overwriteInUseField =
                                    inputPanel.down('[name=overwrite-in-use]');
                                let reuseDatastore =
                                    inputPanel.down('[name=reuse-datastore]').getValue();

                                uuidEditField.setDisabled(!isRemovable);
                                uuidEditField.allowBlank = !isRemovable;
                                uuidEditField.setValue('');

                                bucketField.setDisabled(!isS3);
                                bucketField.allowBlank = !isS3;
                                bucketField.setValue('');

                                s3ClientSelector.setDisabled(!isS3);
                                s3ClientSelector.allowBlank = !isS3;
                                s3ClientSelector.setValue('');

                                overwriteInUseField.setHidden(!isS3);
                                overwriteInUseField.setDisabled(!reuseDatastore);
                                overwriteInUseField.setValue(false);

                                if (isRemovable) {
                                    pathField.setFieldLabel(gettext('Path on Device'));
                                    pathField.setEmptyText(gettext('A relative path'));
                                } else if (isS3) {
                                    pathField.setFieldLabel(gettext('Local Cache'));
                                    pathField.setEmptyText(gettext('An absolute path'));
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
                    {
                        xtype: 'pbsS3ClientSelector',
                        name: 's3client',
                        fieldLabel: gettext('S3 Endpoint ID'),
                        disabled: true,
                        cbind: {
                            editable: '{isCreate}',
                        },
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
                    {
                        xtype: 'proxmoxtextfield',
                        name: 'bucket',
                        fieldLabel: gettext('Bucket'),
                        allowBlank: false,
                        disabled: true,
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
                        listeners: {
                            change: function (checkbox, selected) {
                                let inputPanel = checkbox.up('inputpanel');
                                let overwriteInUseField =
                                    inputPanel.down('[name=overwrite-in-use]');
                                overwriteInUseField.setDisabled(!selected);
                                overwriteInUseField.setValue(false);
                            },
                        },
                    },
                ],
                advancedColumn2: [
                    {
                        xtype: 'checkbox',
                        name: 'overwrite-in-use',
                        fieldLabel: gettext('Overwrite in-use marker'),
                        hidden: true,
                        disabled: true,
                    },
                ],

                onGetValues: function (values) {
                    let me = this;

                    if (me.isCreate) {
                        // New datastores default to using the notification system
                        values['notification-mode'] = 'notification-system';

                        if (values.s3client) {
                            let s3BackendConf = {
                                type: 's3',
                                client: values.s3client,
                                bucket: values.bucket,
                            };
                            values.backend = PBS.Utils.printPropertyString(s3BackendConf);
                        }
                    }

                    delete values.s3client;
                    delete values.bucket;

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
