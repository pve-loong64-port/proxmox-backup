Ext.define('pve-rrd-datastore', {
    extend: 'Ext.data.Model',
    fields: [
        'used',
        'total',
        {
            name: 'unpriv-total', // Can't reuse 'total' here as that creates a stack overflow
            calculate: function (data) {
                let used = data.used;
                let avail = data.available;

                if (avail && used) {
                    return avail + used;
                }

                return data.total;
            },
        },
        'available',
        'read_ios',
        'read_bytes',
        'write_ios',
        'write_bytes',
        'io_ticks',
        's3/uploaded',
        's3/downloaded',
        's3/total/uploaded',
        's3/total/downloaded',
        's3/total/get',
        's3/total/put',
        's3/total/post',
        's3/total/head',
        's3/total/delete',
        {
            name: 'io_delay',
            calculate: function (data) {
                let ios = 0;
                if (data.read_ios !== undefined) {
                    ios += data.read_ios;
                }
                if (data.write_ios !== undefined) {
                    ios += data.write_ios;
                }
                if (data.io_ticks === undefined) {
                    return undefined;
                } else if (ios === 0) {
                    return 0;
                }
                return (data.io_ticks * 1000.0) / ios;
            },
        },
        { type: 'date', dateFormat: 'timestamp', name: 'time' },
    ],
});

Ext.define('PBS.DataStoreInfo', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.pbsDataStoreInfo',

    defaults: {
        xtype: 'pmxInfoWidget',
    },

    bodyPadding: 20,

    items: [
        {
            iconCls: 'fa fa-fw fa-hdd-o',
            title: gettext('Usage'),
            reference: 'usage',
            bind: {
                data: {
                    usage: '{usage}',
                    text: '{usagetext}',
                },
            },
        },
        {
            xtype: 'box',
            html: `<b>${gettext('Backup Count')}</b>`,
            padding: '10 0 5 0',
        },
        {
            iconCls: 'fa fa-fw fa-cube',
            title: gettext('CT'),
            printBar: false,
            bind: {
                data: {
                    text: '{ctcount}',
                },
            },
        },
        {
            iconCls: 'fa fa-fw fa-building',
            title: gettext('Host'),
            printBar: false,
            bind: {
                data: {
                    text: '{hostcount}',
                },
            },
        },
        {
            iconCls: 'fa fa-fw fa-desktop',
            title: gettext('VM'),
            printBar: false,
            bind: {
                data: {
                    text: '{vmcount}',
                },
            },
        },
        {
            xtype: 'box',
            html: `<b>${gettext('Stats from last Garbage Collection')}</b>`,
            padding: '10 0 5 0',
        },
        {
            iconCls: 'fa fa-fw fa-compress',
            title: gettext('Deduplication Factor'),
            printBar: false,
            bind: {
                data: {
                    text: '{deduplication}',
                },
            },
        },
        {
            iconCls: 'fa critical fa-fw fa-exclamation-triangle',
            title: gettext('Bad Chunks'),
            printBar: false,
            bind: {
                data: {
                    text: '{stillbad}',
                },
                visible: '{stillbad}',
            },
        },
    ],
});

Ext.define('PBS.DataStoreS3Stats', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.pbsDataStoreS3Stats',

    defaults: {
        xtype: 'pmxInfoWidget',
    },

    bodyPadding: 20,

    items: [
        {
            xtype: 'box',
            html: `<b>${gettext('S3 traffic:')}</b>`,
            padding: '10 0 5 0',
        },
        {
            iconCls: 'fa fa-fw fa-arrow-up',
            title: gettext('Data uploaded'),
            printBar: false,
            bind: {
                data: {
                    text: '{uploaded}',
                },
            },
        },
        {
            iconCls: 'fa fa-fw fa-arrow-down',
            title: gettext('Data downloaded'),
            printBar: false,
            bind: {
                data: {
                    text: '{downloaded}',
                },
            },
        },
        {
            xtype: 'box',
            html: `<b>${gettext('S3 requests:')}</b>`,
            padding: '10 0 5 0',
        },
        {
            title: gettext('GET'),
            printBar: false,
            bind: {
                data: {
                    text: '{get}',
                },
            },
        },
        {
            title: gettext('PUT'),
            printBar: false,
            bind: {
                data: {
                    text: '{put}',
                },
            },
        },
        {
            title: gettext('POST'),
            printBar: false,
            bind: {
                data: {
                    text: '{post}',
                },
            },
        },
        {
            title: gettext('HEAD'),
            printBar: false,
            bind: {
                data: {
                    text: '{head}',
                },
            },
        },
        {
            title: gettext('DELETE'),
            printBar: false,
            bind: {
                data: {
                    text: '{delete}',
                },
            },
        },
    ],
});

Ext.define('PBS.DataStoreSummary', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.pbsDataStoreSummary',
    mixins: ['Proxmox.Mixin.CBind'],

    layout: 'column',
    scrollable: true,

    bodyPadding: 5,
    defaults: {
        columnWidth: 1,
        padding: 5,
    },

    viewModel: {
        data: {
            countstext: '',
            usage: {},
            stillbad: 0,
            mountpoint: '',
            showS3Stats: false,
        },
    },

    tbar: [
        {
            xtype: 'button',
            text: gettext('Show Connection Information'),
            handler: function () {
                let me = this;
                let datastore = me.up('panel').datastore;
                Ext.create('PBS.window.DatastoreRepoInfo', {
                    datastore,
                    autoShow: true,
                });
            },
        },
        { xtype: 'tbseparator', reference: 'mountButtonSeparator', hidden: true },
        {
            xtype: 'button',
            text: gettext('Unmount'),
            hidden: true,
            itemId: 'unmountButton',
            reference: 'unmountButton',
            handler: function () {
                let me = this;
                let datastore = me.up('panel').datastore;
                Proxmox.Utils.API2Request({
                    url: `/admin/datastore/${datastore}/unmount`,
                    method: 'POST',
                    failure: (response) => Ext.Msg.alert(gettext('Error'), response.htmlStatus),
                    success: function (response, options) {
                        Ext.create('Proxmox.window.TaskViewer', {
                            upid: response.result.data,
                            taskDone: () => {
                                me.up('panel').statusStore.load();
                                Ext.ComponentQuery.query('navigationtree')[0]?.reloadStore();
                            },
                        }).show();
                    },
                });
            },
        },
        {
            xtype: 'button',
            text: gettext('Mount'),
            hidden: true,
            itemId: 'mountButton',
            reference: 'mountButton',
            handler: function () {
                let me = this;
                let datastore = me.up('panel').datastore;
                Proxmox.Utils.API2Request({
                    url: `/admin/datastore/${datastore}/mount`,
                    method: 'POST',
                    failure: (response) => Ext.Msg.alert(gettext('Error'), response.htmlStatus),
                    success: function (response, options) {
                        Ext.create('Proxmox.window.TaskViewer', {
                            upid: response.result.data,
                            taskDone: () => {
                                me.up('panel').statusStore.startUpdate();
                                Ext.ComponentQuery.query('navigationtree')[0]?.reloadStore();
                            },
                        }).show();
                    },
                });
            },
        },
        '->',
        {
            xtype: 'proxmoxRRDTypeSelector',
        },
    ],

    items: [
        {
            xtype: 'container',
            height: 300,
            layout: {
                type: 'hbox',
                align: 'stretch',
            },
            items: [
                {
                    xtype: 'pbsDataStoreInfo',
                    flex: 1,
                    padding: '0 10 0 0',
                    cbind: {
                        title: '{datastore}',
                        datastore: '{datastore}',
                    },
                },
                {
                    xtype: 'pbsDataStoreNotes',
                    flex: 1,
                    padding: '0 10 0 0',
                    cbind: {
                        datastore: '{datastore}',
                    },
                },
                {
                    xtype: 'pbsDataStoreS3Stats',
                    flex: 1,
                    title: gettext('S3 statistics'),
                    bind: {
                        visible: '{showS3Stats}',
                    },
                },
            ],
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('S3 API requests'),
            fields: [
                's3/total/get',
                's3/total/put',
                's3/total/post',
                's3/total/head',
                's3/total/delete',
            ],
            fieldTitles: [
                gettext('GET'),
                gettext('PUT'),
                gettext('POST'),
                gettext('HEAD'),
                gettext('DELETE'),
            ],
            bind: {
                visible: '{showS3Stats}',
            },
            seriesConfig: {
                fill: false,
                style: {
                    lineWidth: 3.0,
                    opacity: 1.0,
                },
            },
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('S3 API download/upload rate (bytes/second)'),
            fields: ['s3/downloaded', 's3/uploaded'],
            fieldTitles: [gettext('Download'), gettext('Upload')],
            bind: {
                visible: '{showS3Stats}',
            },
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('S3 API total download/upload (bytes)'),
            fields: ['s3/total/downloaded', 's3/total/uploaded'],
            fieldTitles: [gettext('Download'), gettext('Upload')],
            bind: {
                visible: '{showS3Stats}',
            },
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('Storage usage (bytes)'),
            name: 'usage-rrd-chart',
            fields: ['unpriv-total', 'used'],
            fieldTitles: [gettext('Total'), gettext('Storage usage')],
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('Transfer Rate (bytes/second)'),
            fields: ['read_bytes', 'write_bytes'],
            fieldTitles: [gettext('Read'), gettext('Write')],
        },
        {
            xtype: 'proxmoxRRDChart',
            title: gettext('Input/Output Operations per Second (IOPS)'),
            fields: ['read_ios', 'write_ios'],
            fieldTitles: [gettext('Read'), gettext('Write')],
        },
        {
            xtype: 'proxmoxRRDChart',
            itemId: 'ioDelayChart',
            hidden: true,
            title: gettext('IO Delay (ms)'),
            fields: ['io_delay'],
            fieldTitles: [gettext('IO Delay')],
        },
    ],

    listeners: {
        activate: function () {
            this.rrdstore.startUpdate();
            this.infoStore.startUpdate();
        },
        afterrender: function () {
            this.statusStore.startUpdate();
        },
        deactivate: function () {
            this.rrdstore.stopUpdate();
            this.infoStore.stopUpdate();
        },
        destroy: function () {
            this.rrdstore.stopUpdate();
            this.statusStore.stopUpdate();
            this.infoStore.stopUpdate();
        },
        resize: function (panel) {
            Proxmox.Utils.updateColumns(panel);
        },
    },

    initComponent: function () {
        let me = this;

        me.rrdstore = Ext.create('Proxmox.data.RRDStore', {
            rrdurl: '/api2/json/admin/datastore/' + me.datastore + '/rrd',
            model: 'pve-rrd-datastore',
        });

        me.statusStore = Ext.create('Proxmox.data.ObjectStore', {
            url: `/api2/json/admin/datastore/${me.datastore}/status`,
            interval: 1000,
        });

        me.infoStore = Ext.create('Proxmox.data.ObjectStore', {
            interval: 5 * 1000,
            url: `/api2/json/admin/datastore/${me.datastore}/status/?verbose=true`,
        });

        let lastRequestFailed = false;
        me.mon(me.statusStore, 'load', (s, records, success) => {
            let mountBtn = me.lookupReferenceHolder().lookupReference('mountButton');
            let unmountBtn = me.lookupReferenceHolder().lookupReference('unmountButton');
            if (!success) {
                lastRequestFailed = true;

                me.statusStore.stopUpdate();
                me.rrdstore.stopUpdate();
                me.infoStore.stopUpdate();
                me.infoStore.load();

                Proxmox.Utils.API2Request({
                    url: `/config/datastore/${me.datastore}`,
                    success: (response) => {
                        let mode = response.result.data['maintenance-mode'];
                        let [type, _message] = PBS.Utils.parseMaintenanceMode(mode);
                        if (!response.result.data['backing-device']) {
                            return;
                        }
                        if (!type || type === 'read-only') {
                            unmountBtn.setDisabled(true);
                            mountBtn.setDisabled(false);
                        } else if (type === 'unmount') {
                            unmountBtn.setDisabled(true);
                            mountBtn.setDisabled(true);
                        } else {
                            unmountBtn.setDisabled(false);
                            mountBtn.setDisabled(false);
                        }
                    },
                });
            } else {
                // only trigger on edges, else we couple our interval to the info one
                if (lastRequestFailed) {
                    me.infoStore.startUpdate();
                    me.rrdstore.startUpdate();
                }
                unmountBtn.setDisabled(false);
                mountBtn.setDisabled(true);
                lastRequestFailed = false;

                let backendType = s.getById('backend-type').data.value;
                if (backendType === 's3') {
                    me.down('[name=usage-rrd-chart]').setTitle(
                        gettext('Local Cache Usage (bytes)'),
                    );
                }
            }
        });

        let sp = Ext.state.Manager.getProvider();
        me.mon(sp, 'statechange', function (provider, key, value) {
            if (key !== 'summarycolumns') {
                return;
            }
            if (!me.rendered) {
                return;
            }
            Proxmox.Utils.updateColumns(me);
        });

        me.callParent();

        Proxmox.Utils.API2Request({
            url: `/config/datastore/${me.datastore}`,
            waitMsgTarget: me.down('pbsDataStoreInfo'),
            success: function (response) {
                let data = response.result.data;

                const removable = !!data['backing-device'];
                me.lookupReferenceHolder()
                    .lookupReference('mountButtonSeparator')
                    .setHidden(!removable);
                me.lookupReferenceHolder().lookupReference('mountButton').setHidden(!removable);
                me.lookupReferenceHolder().lookupReference('unmountButton').setHidden(!removable);

                let path = Ext.htmlEncode(data.path);
                me.down('pbsDataStoreInfo').setTitle(`${me.datastore} (${path})`);
                me.down('pbsDataStoreNotes').setNotes(data.comment);
            },
            failure: function (response) {
                // fallback if e.g. we have no permissions to the config
                let rec = Ext.getStore('pbs-datastore-list').findRecord(
                    'store',
                    me.datastore,
                    0,
                    false,
                    true,
                    true,
                );
                if (rec) {
                    me.down('pbsDataStoreNotes').setNotes(rec.data.comment || '');
                }
            },
        });

        me.mon(me.infoStore, 'load', (store, records, success) => {
            if (!success) {
                Proxmox.Utils.API2Request({
                    url: `/config/datastore/${me.datastore}`,
                    success: function (response) {
                        let maintenanceString = response.result.data['maintenance-mode'];
                        let removable = !!response.result.data['backing-device'];
                        if (!maintenanceString && !removable) {
                            me.down('pbsDataStoreInfo').mask(gettext('Datastore is not available'));
                            return;
                        }

                        let [_type, msg] = PBS.Utils.parseMaintenanceMode(maintenanceString);
                        let isUnplugged = !maintenanceString && removable;
                        let maskMessage = isUnplugged
                            ? gettext('Datastore is not mounted')
                            : `${gettext('Datastore is in maintenance mode')}${msg ? ': ' + msg : ''}`;

                        let maskIcon = isUnplugged
                            ? 'fa pbs-unplugged-mask'
                            : 'fa pbs-maintenance-mask';
                        me.down('pbsDataStoreInfo').mask(maskMessage, maskIcon);
                    },
                });
                return;
            }
            me.down('pbsDataStoreInfo').unmask();

            let vm = me.getViewModel();

            let counts = store.getById('counts').data.value;
            let used = store.getById('used').data.value;
            let total = store.getById('avail').data.value + used;

            let usage = Proxmox.Utils.render_size_usage(used, total, true);
            vm.set('usagetext', usage);
            vm.set('usage', used / total);

            let countstext = function (count) {
                count = count || {};
                return `${count.groups || 0} ${gettext('Groups')}, ${count.snapshots || 0} ${gettext('Snapshots')}`;
            };
            let gcstatus = store.getById('gc-status')?.data.value;
            if (gcstatus) {
                let dedup = PBS.Utils.calculate_dedup_factor(gcstatus);
                vm.set('deduplication', dedup.toFixed(2));
                vm.set('stillbad', gcstatus['still-bad']);
            }
            let s3Stats = store.getById('s3-statistics')?.data.value;
            if (s3Stats) {
                vm.set('uploaded', Proxmox.Utils.format_size(s3Stats.uploaded));
                vm.set('downloaded', Proxmox.Utils.format_size(s3Stats.downloaded));
                vm.set('get', s3Stats.get);
                vm.set('post', s3Stats.post);
                vm.set('delete', s3Stats.delete);
                vm.set('head', s3Stats.head);
                vm.set('put', s3Stats.put);
                vm.set('showS3Stats', true);
            }

            vm.set('ctcount', countstext(counts.ct));
            vm.set('vmcount', countstext(counts.vm));
            vm.set('hostcount', countstext(counts.host));
        });

        me.mon(
            me.rrdstore,
            'load',
            function (store, records, success) {
                let hasIoTicks = records?.some((rec) => rec?.data?.io_ticks !== undefined);
                me.down('#ioDelayChart').setVisible(!success || hasIoTicks);
            },
            undefined,
            { single: true },
        );

        me.query('proxmoxRRDChart').forEach((chart) => {
            chart.setStore(me.rrdstore);
        });
    },
});
