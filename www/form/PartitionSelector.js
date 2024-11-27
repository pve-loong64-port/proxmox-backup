Ext.define('pbs-partition-list', {
    extend: 'Ext.data.Model',
    fields: ['name', 'uuid', 'filesystem', 'devpath', 'size', 'model'],
    proxy: {
	type: 'proxmox',
	url: "/api2/json/nodes/localhost/disks/list?skipsmart=1&include-partitions=1",
	reader: {
	    transform: (rawData) => rawData.data
		.flatMap(disk => (disk.partitions
			.map(part => ({ ...part, model: disk.model })) ?? [])
			.filter(partition => partition.used === 'filesystem' && !!partition.uuid)),
	},
    },
    idProperty: 'devpath',

});

Ext.define('PBS.form.PartitionSelector', {
    extend: 'Proxmox.form.ComboGrid',
    alias: 'widget.pbsPartitionSelector',

    allowBlank: false,
    autoSelect: false,
    submitEmpty: false,
    valueField: 'uuid',
    displayField: 'devpath',

    store: {
	model: 'pbs-partition-list',
	autoLoad: true,
	sorters: 'devpath',
    },
    getSubmitData: function() {
	let me = this;
	let data = null;
	if (!me.disabled && me.submitValue && !me.isFileUpload()) {
	    let val = me.getSubmitValue();
	    if (val !== undefined && val !== null && val !== '') {
		data = {};
		data[me.getName()] = val;
	    } else if (me.getDeleteEmpty()) {
		data = {};
		data.delete = me.getName();
	    }
	}
	return data;
    },
    listConfig: {
	columns: [
	    {
		header: gettext('Path'),
		sortable: true,
		dataIndex: 'devpath',
		renderer: (v, metaData, rec) => Ext.String.htmlEncode(v),
		flex: 1,
	    },
	    {
		header: gettext('Filesystem'),
		sortable: true,
		dataIndex: 'filesystem',
		flex: 1,
	    },
	    {
		header: gettext('Size'),
		sortable: true,
		dataIndex: 'size',
		renderer: Proxmox.Utils.format_size,
		flex: 1,
	    },
	    {
		header: gettext('Model'),
		sortable: true,
		dataIndex: 'model',
		flex: 1,
	    },
	],
	viewConfig: {
	    emptyText: 'No usable partitions present',
	},
    },
});
