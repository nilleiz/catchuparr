-- Apply only to the restored catchuparr Dev database before starting web/Celery.
-- The source database and its backup are never modified.
BEGIN;

UPDATE django_celery_beat_periodictask SET enabled = FALSE;
UPDATE django_celery_beat_periodictasks SET last_update = now();
UPDATE dispatcharr_channels_recurringrecordingrule SET enabled = FALSE;
UPDATE m3u_m3uaccount SET is_active = FALSE;
UPDATE m3u_m3uaccountprofile SET is_active = FALSE;
UPDATE epg_epgsource SET is_active = FALSE;
UPDATE plugins_pluginconfig SET enabled = FALSE;
UPDATE plugins_pluginrepo SET enabled = FALSE;
UPDATE dispatcharr_connect_integration SET enabled = FALSE;
UPDATE dispatcharr_connect_eventsubscription SET enabled = FALSE;

UPDATE core_coresettings
SET value = jsonb_set(value, '{schedule_enabled}', 'false'::jsonb, TRUE)
WHERE key = 'backup_settings';
INSERT INTO core_coresettings (key, name, value)
VALUES (
    'plugin_repo_settings', 'Plugin repository settings',
    '{"refresh_interval_hours":0}'::jsonb
)
ON CONFLICT (key) DO UPDATE SET value = jsonb_set(
    core_coresettings.value, '{refresh_interval_hours}', '0'::jsonb, TRUE
);
UPDATE core_coresettings
SET value = jsonb_set(value, '{series_rules}', '[]'::jsonb, TRUE)
WHERE key = 'dvr_settings';
UPDATE core_coresettings
SET value = jsonb_set(
    jsonb_set(value, '{auto_import_mapped_files}', 'false'::jsonb, TRUE),
    '{enable_ip_lookup}', 'false'::jsonb, TRUE
)
WHERE key = 'system_settings';

COMMIT;
