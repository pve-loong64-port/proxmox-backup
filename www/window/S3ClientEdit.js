Ext.define('PBS.window.S3ClientEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsS3ClientEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    onlineHelp: 'datastore-s3-backend',

    isAdd: true,

    subject: gettext('S3 Endpoint'),

    fieldDefaults: { labelWidth: 120 },

    cbindData: function (initialConfig) {
        let me = this;

        let baseurl = '/api2/extjs/config/s3';
        let id = initialConfig.id;

        me.isCreate = !id;
        me.url = id ? `${baseurl}/${id}` : baseurl;
        me.method = id ? 'PUT' : 'POST';
        me.autoLoad = !!id;
        return {
            passwordEmptyText: me.isCreate ? '' : gettext('Unchanged'),
        };
    },

    items: {
        xtype: 'inputpanel',
        column1: [
            {
                xtype: 'pmxDisplayEditField',
                name: 'id',
                fieldLabel: gettext('S3 Endpoint ID'),
                renderer: Ext.htmlEncode,
                allowBlank: false,
                minLength: 4,
                cbind: {
                    editable: '{isCreate}',
                },
            },
            {
                xtype: 'proxmoxtextfield',
                name: 'endpoint',
                fieldLabel: gettext('Endpoint'),
                allowBlank: false,
                emptyText: gettext('e.g. {{bucket}}.s3.{{region}}.amazonaws.com'),
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext(
                        'IP or FQDN S3 endpoint (allows {{bucket}} or {{region}} templating)',
                    ),
                },
            },
            {
                xtype: 'proxmoxtextfield',
                name: 'port',
                fieldLabel: gettext('Port'),
                emptyText: gettext('default (443)'),
                cbind: {
                    deleteEmpty: '{!isCreate}',
                },
            },
            {
                xtype: 'proxmoxcheckbox',
                name: 'path-style',
                fieldLabel: gettext('Path Style'),
                autoEl: {
                    tag: 'div',
                    'data-qtip': gettext('Use path style over vhost style bucket addressing.'),
                },
                uncheckedValue: false,
                value: false,
            },
        ],

        column2: [
            {
                xtype: 'proxmoxtextfield',
                name: 'region',
                fieldLabel: gettext('Region'),
                emptyText: gettext('default (us-west-1)'),
                cbind: {
                    deleteEmpty: '{!isCreate}',
                },
            },
            {
                xtype: 'proxmoxtextfield',
                name: 'access-key',
                fieldLabel: gettext('Access Key'),
                cbind: {
                    emptyText: '{passwordEmptyText}',
                    allowBlank: '{!isCreate}',
                },
            },
            {
                xtype: 'textfield',
                name: 'secret-key',
                inputType: 'password',
                fieldLabel: gettext('Secret Key'),
                cbind: {
                    emptyText: '{passwordEmptyText}',
                    allowBlank: '{!isCreate}',
                },
            },
        ],

        columnB: [
            {
                xtype: 'proxmoxtextfield',
                name: 'fingerprint',
                fieldLabel: gettext('Fingerprint'),
                emptyText: gettext(
                    "Server certificate's SHA-256 fingerprint, required for self-signed certificates",
                ),
                cbind: {
                    deleteEmpty: '{!isCreate}',
                },
            },
        ],

        advancedColumn1: [
            {
                xtype: 'proxmoxKVComboBox',
                name: 'provider-quirks',
                fieldLabel: gettext('Provider specific quirks'),
                value: '__default__',
                defaultValue: '__default__',
                comboItems: [
                    ['__default__', 'None (default)'],
                    ['skip-if-none-match-header', 'Skip If-None-Match header'],
                ],
                cbind: {
                    deleteEmpty: '{!isCreate}',
                },
            },
        ],
    },

    getValues: function () {
        let me = this;
        let values = me.callParent(arguments);

        if (values.delete && !Ext.isArray(values.delete)) {
            values.delete = values.delete.split(',');
        }
        PBS.Utils.delete_if_default(values, 'path-style', false, me.isCreate);

        let https_scheme_prefix = 'https://';
        if (values.endpoint.startsWith(https_scheme_prefix)) {
            values.endpoint = values.endpoint.slice(https_scheme_prefix.length);
        }
        if (values['access-key'] === '') {
            delete values['access-key'];
        }

        if (values['secret-key'] === '') {
            delete values['secret-key'];
        }

        return values;
    },
});
