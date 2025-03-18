Ext.define('PBS.window.SyncJobEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsSyncJobEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    userid: undefined,

    onlineHelp: 'syncjobs',

    isAdd: true,

    subject: gettext('Sync Job - Pull Direction'),

    bodyPadding: 0,
    width: 720,

    fieldDefaults: { labelWidth: 120 },
    defaultFocus: 'proxmoxtextfield[name=comment]',

    cbindData: function(initialConfig) {
	let me = this;

	let baseurl = '/api2/extjs/config/sync';
	let id = initialConfig.id;

	me.isCreate = !id;
	me.url = id ? `${baseurl}/${id}` : baseurl;
	me.method = id ? 'PUT' : 'POST';
	me.autoLoad = !!id;
	me.scheduleValue = id ? null : 'hourly';
	me.authid = id ? null : Proxmox.UserName;
	me.editDatastore = me.datastore === undefined && me.isCreate;

	if (me.syncDirection === 'push') {
	    me.subject = gettext('Sync Job - Push Direction');
	    me.syncDirectionPush = true;
	    me.syncRemoteLabel = gettext('Target Remote');
	    me.syncRemoteDatastore = gettext('Target Datastore');
	    me.syncRemoteNamespace = gettext('Target Namespace');
	    me.syncLocalOwner = gettext('Local User');
	    // Sync direction request parameter is only required for creating new jobs,
	    // for edit and delete it is derived from the job config given by it's id.
	    if (me.isCreate) {
		me.extraRequestParams = {
		    "sync-direction": 'push',
		};
	    }
	} else {
	    me.subject = gettext('Sync Job - Pull Direction');
	    me.syncDirectionPush = false;
	    me.syncRemoteLabel = gettext('Source Remote');
	    me.syncRemoteDatastore = gettext('Source Datastore');
	    me.syncRemoteNamespace = gettext('Source Namespace');
	    me.syncLocalOwner = gettext('Local Owner');
	}

	return { };
    },

    controller: {
	xclass: 'Ext.app.ViewController',
	control: {
	    'pbsDataStoreSelector[name=store]': {
		change: 'storeChange',
	    },
	},

	storeChange: function(field, value) {
	    let view = this.getView();
	    let nsSelector = view.down('pbsNamespaceSelector[name=ns]');
	    nsSelector.setDatastore(value);
	},

	init: function() {
	    let view = this.getView();
	    // Cannot use cbind to hide rate limit field depending on sync direction,
	    // set it in init as workaround.
	    if (view.syncDirectionPush) {
		view.down('pmxBandwidthField[name=rate-in]').setHidden(true);
		if (view.datastore !== undefined) {
		    let localNs = view.down('pbsNamespaceSelector[name=ns]').getValue();
		    view.down('pbsGroupFilter').setLocalNamespace(view.datastore, localNs);
		}
	    } else {
		view.down('pmxBandwidthField[name=rate-out]').setHidden(true);
	    }
	},
    },

    setValues: function(values) {
	let me = this;
	if (values.id && !values.remote) {
	    values.location = 'local';
	} else {
	    values.location = 'remote';
	}
	me.callParent([values]);
    },

    getVisibleRateLimitField: function() {
	let me = this;
	if (me.syncDirectionPush) {
	    return me.down('field[name=rate-out]');
	} else {
	    return me.down('field[name=rate-in]');
	}
    },

    items: {
	xtype: 'tabpanel',
	bodyPadding: 10,
	border: 0,
	items: [
	    {
		xtype: 'inputpanel',
		title: 'Options',
		onGetValues: function(values) {
		    let me = this;

		    if (!values.id && me.up('pbsSyncJobEdit').isCreate) {
			values.id = 's-' + Ext.data.identifier.Uuid.Global.generate().slice(0, 13);
		    }
		    if (!me.isCreate) {
			PBS.Utils.delete_if_default(values, 'rate-in');
			PBS.Utils.delete_if_default(values, 'rate-out');
			PBS.Utils.delete_if_default(values, 'remote');
			if (typeof values.delete === 'string') {
			    values.delete = values.delete.split(',');
			}
		    }
		    return values;
		},
		cbind: {
		    isCreate: '{isCreate}', // pass it through
		},
		column1: [
		    {
			xtype: 'pmxDisplayEditField',
			fieldLabel: gettext('Local Datastore'),
			name: 'store',
			submitValue: true,
			cbind: {
			    editable: '{editDatastore}',
			    value: '{datastore}',
			},
			editConfig: {
			    xtype: 'pbsDataStoreSelector',
			    allowBlank: false,
			},
			listeners: {
			    change: function(field, localStore) {
				let me = this;
				let view = me.up('pbsSyncJobEdit');
				if (view.syncDirectionPush) {
				    let localNs = view.down('pbsNamespaceSelector[name=ns]').getValue();
				    view.down('pbsGroupFilter').setLocalNamespace(localStore, localNs);
				}
			    },
			},
		    },
		    {
			xtype: 'pbsNamespaceSelector',
			fieldLabel: gettext('Local Namespace'),
			name: 'ns',
			cbind: {
			    datastore: '{datastore}',
			},
			listeners: {
			    change: function(field, localNs) {
				let me = this;
				let view = me.up('pbsSyncJobEdit');

				if (view.syncDirectionPush) {
				    let localStore = view.down('field[name=store]').getValue();
				    view.down('pbsGroupFilter').setLocalNamespace(localStore, localNs);
				}

				let remoteNs = view.down('pbsRemoteNamespaceSelector[name=remote-ns]').getValue();
				let maxDepthField = view.down('field[name=max-depth]');
				maxDepthField.setLimit(localNs, remoteNs);
				maxDepthField.validate();
			    },
			},
		    },
		    {
			xtype: 'pbsAuthidSelector',
			name: 'owner',
			cbind: {
			    fieldLabel: '{syncLocalOwner}',
			    value: '{authid}',
			    deleteEmpty: '{!isCreate}',
			},
		    },
		    {
			xtype: 'pbsCalendarEvent',
			name: 'schedule',
			fieldLabel: gettext('Sync Schedule'),
			emptyText: gettext('none (disabled)'),
			cbind: {
			    deleteEmpty: '{!isCreate}',
			    value: '{scheduleValue}',
			},
		    },
		    {
			xtype: 'pmxBandwidthField',
			name: 'rate-in',
			fieldLabel: gettext('Rate Limit'),
			emptyText: gettext('Unlimited'),
			submitAutoScaledSizeUnit: true,
			// NOTE: handle deleteEmpty in onGetValues due to bandwidth field having a cbind too,
			// further hide rate limit field depending on sync direction in controller init.
		    },
		    {
			xtype: 'pmxBandwidthField',
			name: 'rate-out',
			fieldLabel: gettext('Rate Limit'),
			emptyText: gettext('Unlimited'),
			submitAutoScaledSizeUnit: true,
			// NOTE: handle deleteEmpty in onGetValues due to bandwidth field having a cbind too,
			// further hide rate limit field depending on sync direction in controller init.
		    },
		],

		column2: [
		    {
			xtype: 'radiogroup',
			fieldLabel: gettext('Location'),
			defaultType: 'radiofield',
			cbind: {
			    disabled: '{syncDirectionPush}',
			},
			items: [
			    {
				boxLabel: 'Local',
				name: 'location',
				inputValue: 'local',
				submitValue: false,
			    },
			    {
				boxLabel: 'Remote',
				name: 'location',
				inputValue: 'remote',
				submitValue: false,
				checked: true,
			    },
			],
			listeners: {
			    change: function(_group, radio) {
				let me = this;
				let form = me.up('pbsSyncJobEdit');
				let nsField = form.down('field[name=remote-ns]');
				let rateLimitField = form.getVisibleRateLimitField();
				let remoteField = form.down('field[name=remote]');
				let storeField = form.down('field[name=remote-store]');

				if (!storeField.value) {
				    nsField.clearValue();
				    nsField.setDisabled(true);
				}

				let isLocalSync = radio.location === 'local';
				rateLimitField.setDisabled(isLocalSync);
				remoteField.allowBlank = isLocalSync;
				remoteField.setDisabled(isLocalSync);
				storeField.setDisabled(!isLocalSync && !remoteField.value);
				if (isLocalSync === !!remoteField.value) {
				    remoteField.clearValue();
				}

				if (isLocalSync) {
				    storeField.setDisabled(false);
				    rateLimitField.setValue(null);
				    storeField.setRemote(null, true);
				} else {
				    storeField.clearValue();
				    remoteField.validate();
				}
			    },
			},
		    },
		    {
			xtype: 'pbsRemoteSelector',
			cbind: {
			    fieldLabel: '{syncRemoteLabel}',
			},
			allowBlank: false,
			name: 'remote',
			skipEmptyText: true,
			listeners: {
			    change: function(f, value) {
				let me = this;
				let remoteStoreField = me.up('pbsSyncJobEdit').down('field[name=remote-store]');
				remoteStoreField.setRemote(value);
				let rateLimitField = me.up('pbsSyncJobEdit').getVisibleRateLimitField();
				rateLimitField.setDisabled(!value);
				if (!value) {
				    rateLimitField.setValue(null);
				}
				let remoteNamespaceField = me.up('pbsSyncJobEdit').down('field[name=remote-ns]');
				remoteNamespaceField.setRemote(value);
			    },
			},
		    },
		    {
			xtype: 'pbsRemoteStoreSelector',
			allowBlank: false,
			autoSelect: false,
			name: 'remote-store',
			cbind: {
			    datastore: '{datastore}',
			    fieldLabel: '{syncRemoteDatastore}',
			},
			listeners: {
			    change: function(field, value) {
				let me = this;
				let remoteField = me.up('pbsSyncJobEdit').down('field[name=remote]');
				let remote = remoteField.getValue();
				let remoteNamespaceField = me.up('pbsSyncJobEdit').down('field[name=remote-ns]');
				remoteNamespaceField.setRemote(remote);
				remoteNamespaceField.setRemoteStore(value);

				let view = me.up('pbsSyncJobEdit');
				if (!view.syncDirectionPush) {
				    me.up('tabpanel').down('pbsGroupFilter').setRemoteDatastore(remote, value);
				} else {
				    let localStore = me.up('pbsSyncJobEdit').down('field[name=store]').getValue();
				    me.up('tabpanel').down('pbsGroupFilter').setLocalDatastore(localStore);
				}
			    },
			},
		    },
		    {
			cbind: {
			    fieldLabel: '{syncRemoteNamespace}',
			},
			xtype: 'pbsRemoteNamespaceSelector',
			allowBlank: true,
			autoSelect: false,
			name: 'remote-ns',
			disabled: true,
			listeners: {
			    change: function(field, remoteNs) {
				let me = this;
				let view = me.up('pbsSyncJobEdit');

				let remote = view.down('field[name=remote]').getValue();
				let remoteStore = view.down('field[name=remote-store]').getValue();

				if (!view.syncDirectionPush) {
				    me.up('tabpanel').down('pbsGroupFilter').setRemoteNamespace(remote, remoteStore, remoteNs);
				}

				let localNs = view.down('pbsNamespaceSelector[name=ns]').getValue();
				let maxDepthField = view.down('field[name=max-depth]');
				maxDepthField.setLimit(localNs, remoteNs);
				maxDepthField.validate();
			    },
			},
		    },
		    {
			xtype: 'pbsNamespaceMaxDepthReduced',
			name: 'max-depth',
			fieldLabel: gettext('Max. Depth'),
			cbind: {
			    deleteEmpty: '{!isCreate}',
			},
		    },
		    {
			xtype: 'proxmoxcheckbox',
			name: 'remove-vanished',
			fieldLabel: gettext('Remove Vanished'),
			autoEl: {
			    tag: 'div',
			    'data-qtip': gettext('Remove snapshots from sync target datastore if they vanished from source datastore?'),
			},
			uncheckedValue: false,
			value: false,
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
			xtype: 'pmxDisplayEditField',
			name: 'id',
			fieldLabel: gettext('Job ID'),
			emptyText: gettext('Autogenerate'),
			allowBlank: true,
			regex: PBS.Utils.SAFE_ID_RE,
			cbind: {
			    editable: '{isCreate}',
			},
		    },
		    {
			xtype: 'pbsPruneKeepInput',
			name: 'transfer-last',
			fieldLabel: gettext('Transfer Last'),
			emptyText: gettext('all'),
			autoEl: {
			    tag: 'div',
			    'data-qtip': gettext('The maximum amount of snapshots to be transferred (per group)'),
			},
			cbind: {
			    deleteEmpty: '{!isCreate}',
			},
		    },
		    {
			xtype: 'proxmoxcheckbox',
			fieldLabel: gettext('Re-sync Corrupt'),
			name: 'resync-corrupt',
			autoEl: {
			    tag: 'div',
			    'data-qtip': gettext('Re-sync snapshots, whose verification failed.'),
			},
			cbind: {
			    disabled: '{syncDirectionPush}',
			},
			uncheckedValue: false,
			value: false,
		    },
		],
		advancedColumn2: [
		    {
			xtype: 'proxmoxcheckbox',
			name: 'encrypted-only',
			fieldLabel: gettext('Encrypted Only'),
			autoEl: {
			    tag: 'div',
			    'data-qtip': gettext('Sync only encrypted backup snapshots, exclude others.'),
			},
			uncheckedValue: false,
			value: false,
		    },
		    {
			xtype: 'proxmoxcheckbox',
			name: 'verified-only',
			fieldLabel: gettext('Verified Only'),
			autoEl: {
			    tag: 'div',
			    'data-qtip': gettext('Sync only verified backup snapshots, exclude others.'),
			},
			uncheckedValue: false,
			value: false,
		    },
		],
	    },
	    {
		xtype: 'inputpanel',
		onGetValues: function(values) {
		    let me = this;
		    PBS.Utils.delete_if_default(values, 'group-filter');
		    if (Ext.isArray(values['group-filter'])) {
			if (values['group-filter'].length === 0) {
			    delete values['group-filter'];
			    values.delete = 'group-filter';
			} else {
			    // merge duplicates
			    values['group-filter'] = [...new Set(values['group-filter'])];
			}
		    }
		    if (me.isCreate) {
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
	],
    },
});
