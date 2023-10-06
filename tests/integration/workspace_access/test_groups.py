import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    listing_wrapper,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager

logger = logging.getLogger(__name__)


def test_prepare_environment(ws, make_ucx_group):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_state
    for _info in group_migration_state.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        _ws_members = sorted([m.value for m in _ws.members])
        _backup_members = sorted([m.value for m in _backup.members])
        assert _ws_members == _backup_members

    for _info in group_migration_state.groups:
        # cleanup side-effect
        ws.groups.delete(_info.backup.id)


def test_prepare_environment_no_groups_selected(ws, make_ucx_group, make_group, make_acc_group):
    make_group()
    make_acc_group()
    for_test = [make_ucx_group(), make_ucx_group()]

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_state
    for _info in group_migration_state.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        # https://github.com/databricks/databricks-sdk-py/pull/361 may fix the NPE gotcha with empty members
        _ws_members = sorted([m.value for m in _ws.members]) if _ws.members is not None else []
        _backup_members = sorted([m.value for m in _backup.members]) if _backup.members is not None else []
        assert _ws_members == _backup_members

    for g, _ in for_test:
        assert group_migration_state.get_by_workspace_group_name(g.display_name) is not None

    for _info in group_migration_state.groups:
        # cleanup side-effect
        ws.groups.delete(_info.backup.id)


def test_replace_workspace_groups_with_account_groups(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_group,
    make_acc_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_table,
):
    ws_group, _ = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    logger.info(f"Cluster policy: {ws.config.host}#setting/clusters/cluster-policies/view/{cluster_policy.policy_id}")

    dummy_table = make_table()
    sql_backend.execute(f"GRANT SELECT ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_info = group_manager.migration_state.get_by_workspace_group_name(ws_group.display_name)

    generic_permissions = GenericPermissionsSupport(
        ws, [listing_wrapper(ws.cluster_policies.list, "policy_id", "cluster-policies")]
    )
    permission_manager = PermissionManager(
        sql_backend, inventory_schema, [generic_permissions], {"cluster-policies": generic_permissions}
    )
    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)

    permission_manager.inventorize_permissions()

    table_permissions = grants.for_table_info(dummy_table)
    print(table_permissions)

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="backup")

    policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.workspace.display_name]
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    group_manager.replace_workspace_groups_with_account_groups()

    table_permissions = grants.for_table_info(dummy_table)
    print(table_permissions)

    policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert group_info.workspace.display_name not in policy_permissions
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="account")

    table_permissions = grants.for_table_info(dummy_table)
    print(table_permissions)

    policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.account.display_name]
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    # TODO: check hive grants as well

    for _info in group_manager.migration_state.groups:
        ws.groups.delete(_info.backup.id)

    policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert group_info.backup.display_name not in policy_permissions


def test_group_listing(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.display_name in [g.display_name for g in manager._workspace_groups]
    assert acc_group.display_name in [g.display_name for g in manager._account_groups]


def test_id_validity(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.id == manager._get_group(ws_group.display_name, "workspace").id
    assert acc_group.id == manager._get_group(acc_group.display_name, "account").id
