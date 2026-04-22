Ext.define('PBS.window.NamespaceMove', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsNamespaceMove',
    mixins: ['Proxmox.Mixin.CBind'],

    onlineHelp: 'storage-move-namespaces-groups',

    submitText: gettext('Move'),
    isCreate: true,
    showTaskViewer: true,

    cbind: {
        url: '/api2/extjs/admin/datastore/{datastore}/move-namespace',
        title: (get) => Ext.String.format(gettext("Move Namespace '{0}'"), get('namespace')),
    },
    method: 'POST',

    width: 450,
    fieldDefaults: {
        labelWidth: 120,
    },

    cbindData: function (initialConfig) {
        let ns = initialConfig.namespace ?? '';
        let parts = ns.split('/');
        let nsName = parts.pop();
        return {
            nsName,
            nsParent: parts.join('/'),
        };
    },

    // Compose the submitted target namespace from the current field values.
    getTargetNs: function () {
        let me = this;
        let parent = me.down('[name=parent]').getValue() || '';
        let name = me.down('[name=name]').getValue();
        return parent ? `${parent}/${name}` : name;
    },

    // Returns the target-ns path that was submitted, for use by the caller after success.
    getNewNamespace: function () {
        return this.getTargetNs();
    },

    items: {
        xtype: 'inputpanel',
        onGetValues: function (values) {
            let win = this.up('window');
            let result = {
                ns: win.namespace,
                'target-ns': win.getTargetNs(),
                'delete-source': values['delete-source'],
                'merge-groups': values['merge-groups'],
            };
            if (values['max-depth'] !== undefined && values['max-depth'] !== '') {
                result['max-depth'] = values['max-depth'];
            }
            PBS.Utils.delete_if_default(result, 'delete-source', true, true);
            PBS.Utils.delete_if_default(result, 'merge-groups', true, true);
            return result;
        },
        items: [
            {
                xtype: 'displayfield',
                fieldLabel: gettext('Namespace'),
                cbind: {
                    value: '{namespace}',
                },
            },
            {
                xtype: 'pbsNamespaceSelector',
                name: 'parent',
                fieldLabel: gettext('New Parent'),
                allowBlank: true,
                cbind: {
                    datastore: '{datastore}',
                    excludeNs: '{namespace}',
                    value: '{nsParent}',
                },
            },
            {
                xtype: 'proxmoxtextfield',
                name: 'name',
                fieldLabel: gettext('New Name'),
                allowBlank: false,
                maxLength: 31,
                regex: PBS.Utils.SAFE_ID_RE,
                regexText: gettext("Only alpha numerical, '_' and '-' (if not at start) allowed"),
                cbind: {
                    value: '{nsName}',
                },
            },
        ],
        advancedItems: [
            {
                xtype: 'pbsNamespaceMaxDepthReduced',
                name: 'max-depth',
                fieldLabel: gettext('Max Depth'),
                emptyText: gettext('Unlimited'),
                listeners: {
                    afterrender: function (field) {
                        let win = field.up('window');
                        field.setLimit(win.namespace, null);
                    },
                },
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext('Limit how many levels of child namespaces to include. Leave empty to move the entire subtree.'),
                },
            },
            {
                xtype: 'proxmoxcheckbox',
                name: 'merge-groups',
                fieldLabel: gettext('Merge Groups'),
                checked: true,
                uncheckedValue: false,
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext('Merge snapshots into existing groups with the same name in the target namespace. Requires matching ownership and non-overlapping snapshot times.'),
                },
            },
            {
                xtype: 'proxmoxcheckbox',
                name: 'delete-source',
                fieldLabel: gettext('Delete Source'),
                checked: true,
                uncheckedValue: false,
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext('Remove the empty source namespace directories after moving all groups. Uncheck to keep the namespace structure.'),
                },
            },
        ],
    },
});
