-- 014_fix_can_manage_user_code.sql
--
-- database/02_config.sql originally seeded two actions for the same
-- permission: id 3 'CAN_MANGE_USER' (misspelled, the one actually wired
-- into every route/check in main.go, internal/api/user_controller.go, and
-- frontend/src/components/layout/Sidebar.tsx) and id 12
-- 'CAN_MANAGE_USERS' (correctly spelled but never referenced anywhere) —
-- an accidental duplicate, not two distinct permissions. Fixes the typo on
-- the row that's actually in use (an UPDATE, not a delete+recreate, so
-- existing role_actions grants on id 3 survive untouched) and removes the
-- unused duplicate. Safe to run on a database that never had the
-- duplicate (both statements affect zero rows in that case).
--
-- database/02_config.sql itself is also fixed to seed the corrected
-- spelling directly and drop the duplicate row, so a completely fresh
-- database never needs this migration at all — it exists only for a
-- database provisioned before this fix.

UPDATE public.actions SET code = 'CAN_MANAGE_USER' WHERE code = 'CAN_MANGE_USER';

DELETE FROM public.role_actions
WHERE action_id IN (SELECT id FROM public.actions WHERE code = 'CAN_MANAGE_USERS');

DELETE FROM public.actions WHERE code = 'CAN_MANAGE_USERS';
