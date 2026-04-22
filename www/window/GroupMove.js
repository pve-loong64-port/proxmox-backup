Ext.define('PBS.window.GroupMove', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsGroupMove',
    mixins: ['Proxmox.Mixin.CBind'],

    onlineHelp: 'storage-move-namespaces-groups',

    isCreate: true,
    submitText: gettext('Move'),
    showTaskViewer: true,

    cbind: {
        url: '/api2/extjs/admin/datastore/{datastore}/move-group',
        title: (get) => Ext.String.format(gettext("Move Backup Group '{0}'"), get('group')),
    },
    method: 'POST',

    width: 400,
    fieldDefaults: {
        labelWidth: 120,
    },

    items: {
        xtype: 'inputpanel',
        onGetValues: function (values) {
            let win = this.up('window');
            let result = {
                'backup-type': win.backupType,
                'backup-id': win.backupId,
                'target-ns': values['target-ns'] || '',
            };
            if (win.namespace && win.namespace !== '') {
                result.ns = win.namespace;
            }
            result['merge-group'] = values['merge-group'];
            PBS.Utils.delete_if_default(result, 'merge-group', true, true);
            return result;
        },
        items: [
            {
                xtype: 'displayfield',
                fieldLabel: gettext('Group'),
                cbind: {
                    value: '{group}',
                },
            },
            {
                xtype: 'pbsNamespaceSelector',
                name: 'target-ns',
                fieldLabel: gettext('Target Namespace'),
                allowBlank: true,
                cbind: {
                    datastore: '{datastore}',
                },
            },
        ],
        advancedItems: [
            {
                xtype: 'proxmoxcheckbox',
                name: 'merge-group',
                fieldLabel: gettext('Merge Group'),
                checked: true,
                uncheckedValue: false,
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext('If a group with the same name already exists in the target namespace, merge snapshots into it. Requires matching ownership and non-overlapping snapshot times.'),
                },
            },
        ],
    },
});
