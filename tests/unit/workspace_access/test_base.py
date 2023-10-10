from functools import partial

from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.base import Applier, Permissions
from databricks.labs.ucx.workspace_access.groups import (
    GroupMigrationState,
    MigrationGroupInfo,
)


def test_applier():
    class SampleApplier(Applier):
        def is_item_relevant(self, item: Permissions, migration_state: GroupMigrationState) -> bool:
            workspace_groups = [info.workspace.display_name for info in migration_state.groups]
            return item.object_id in workspace_groups

        def _get_apply_task(self, _, __, ___):
            def test_task():
                print("here!")

            return partial(test_task)

    applier = SampleApplier()
    positive_item = Permissions(object_id="test", object_type="test", raw="test")
    migration_state = GroupMigrationState()
    migration_state.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test", id="test"),
            account=iam.Group(display_name="test", id="test-acc"),
            backup=iam.Group(display_name="db-temp-test", id="test-backup"),
        )
    )

    task = applier.get_apply_task(positive_item, migration_state, "backup")
    assert task.func.__name__ == "test_task"

    negative_item = Permissions(object_id="not-here", object_type="test", raw="test")
    new_task = applier.get_apply_task(negative_item, migration_state, "backup")
    new_task.func()


def test_mapping():
    permissions1 = [
        iam.Permission(permission_level=iam.PermissionLevel.CAN_BIND),
        iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE),
    ]
    response1 = iam.AccessControlResponse(all_permissions=permissions1, user_name="test1212")

    permissions2 = [iam.Permission(permission_level=iam.PermissionLevel.CAN_ATTACH_TO)]
    response2 = iam.AccessControlResponse(all_permissions=permissions2, group_name="data-engineers")

    permissions3 = [iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS)]
    response3 = iam.AccessControlResponse(all_permissions=permissions3, service_principal_name="sp1")

    object_permissions = iam.ObjectPermissions(access_control_list=[response1, response2, response3])

    results = Applier().response_to_request(object_permissions.access_control_list)

    assert results == [
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_BIND, user_name="test1212"),
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_MANAGE, user_name="test1212"),
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_ATTACH_TO, group_name="data-engineers"),
        iam.AccessControlRequest(
            permission_level=iam.PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS, service_principal_name="sp1"
        ),
    ]
