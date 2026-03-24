Ext.define('PBS.form.EncryptionKeySelector', {
    extend: 'Ext.form.field.ComboBox',
    alias: 'widget.pbsEncryptionKeySelector',

    queryMode: 'local',

    valueField: 'id',
    displayField: 'id',

    emptyText: gettext('None'),

    listConfig: {
        columns: [
            {
                dataIndex: 'id',
                header: gettext('Key ID'),
                sortable: true,
                flex: 1,
            },
            {
                dataIndex: 'created',
                header: gettext('Created'),
                sortable: true,
                renderer: Proxmox.Utils.render_timestamp,
                flex: 1,
            },
            {
                dataIndex: 'archived-at',
                header: gettext('Archived'),
                renderer: (val) => (val ? Proxmox.Utils.render_timestamp(val) : ''),
                sortable: true,
                flex: 1,
            },
        ],
        emptyText: `<div class="x-grid-empty">${gettext('No key accessible.')}</div>`,
    },

    config: {
        deleteEmpty: true,
        extraRequestParams: {},
    },
    // override framework function to implement deleteEmpty behaviour
    getSubmitData: function () {
        let me = this;

        let data = null;
        if (!me.disabled && me.submitValue) {
            let val = me.getSubmitValue();
            if (val !== null && val !== '') {
                data = {};
                data[me.getName()] = val;
            } else if (me.getDeleteEmpty()) {
                data = {};
                data.delete = me.getName();
            }
        }

        return data;
    },

    triggers: {
        clear: {
            cls: 'pmx-clear-trigger',
            weight: -1,
            hidden: true,
            handler: function () {
                this.triggers.clear.setVisible(false);
                this.setValue('');
            },
        },
    },

    listeners: {
        change: function (field, value) {
            let canClear = (value ?? '') !== '';
            field.triggers.clear.setVisible(canClear);
        },
    },

    initComponent: function () {
        let me = this;

        me.store = Ext.create('Ext.data.Store', {
            model: 'pbs-encryption-keys',
            autoLoad: true,
            proxy: {
                type: 'proxmox',
                timeout: 30 * 1000,
                url: `/api2/json/config/encryption-keys`,
                extraParams: me.extraRequestParams,
            },
        });

        me.callParent();
    },
});
