Ext.define('PBS.config.SyncPullPush', {
    extend: 'Ext.panel.Panel',
    alias: 'widget.pbsSyncJobPullPushView',
    title: gettext('Sync Jobs'),

    mixins: ['Proxmox.Mixin.CBind'],

    layout: {
	type: 'vbox',
	align: 'stretch',
	multi: true,
	bodyPadding: 5,
    },
    defaults: {
	collapsible: false,
	margin: 5,
    },
    scrollable: true,
    items: [
	{
	    xtype: 'pbsSyncJobView',
	    itemId: 'syncJobsPull',
	    syncDirection: 'pull',
	    cbind: {
		datastore: '{datastore}',
	    },
	    minHeight: 125, // shows at least one line of content
	},
	{
	    xtype: 'splitter',
	    performCollapse: false,
	},
	{
	    xtype: 'pbsSyncJobView',
	    itemId: 'syncJobsPush',
	    syncDirection: 'push',
	    cbind: {
		datastore: '{datastore}',
	    },
	    flex: 1,
	    minHeight: 125, // shows at least one line of content
	},
    ],
    initComponent: function() {
	let me = this;

	let subPanelIds = me.items.map(el => el.itemId).filter(id => !!id);

	me.callParent();

	for (const itemId of subPanelIds) {
	    let component = me.getComponent(itemId);
	    component.relayEvents(me, ['activate', 'deactivate', 'destroy']);
	}
    },

    cbindData: function(initialConfig) {
        let me = this;
        me.datastore = initialConfig.datastore ? initialConfig.datastore : undefined;
    },
});
