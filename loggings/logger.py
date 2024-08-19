from collections import defaultdict
import contextvars
import json

from django.core import serializers
from django.db.models import Model

from .constants import ACTION_CREATE, ACTION_DELETE, ACTION_UPDATE
from .models import Log, LogExtra


# Stored log id's per model instance, for use with squashing a log sequence
log_history = contextvars.ContextVar("loggings__log_history")


def begin_log_sequence():
    """Begin recording log id's per model instance."""
    return log_history.set(defaultdict(list))


def end_log_sequence(token):
    """End recording log id's per model instance."""
    log_history.reset(token)


class Logger(object):
    actions = (ACTION_CREATE, ACTION_UPDATE, ACTION_DELETE)
    extras = None
    previous_obj = None
    user = None

    def __init__(self, action, current_obj, previous_obj=None, user=None,
                 extras=None):

        try:
            action = int(action)
        except ValueError:
            raise ValueError("Action must be an integer.")

        if action not in self.actions:
            raise Exception(
                "Action must be an integer in {0}".format(self.actions))
        self.action = action

        if not isinstance(current_obj, Model):
            raise TypeError("current_obj must be a Django model instance.")
        self.current_obj = current_obj

        if previous_obj:
            if not isinstance(previous_obj, Model):
                raise TypeError(
                    "previous_obj must be a Django model instance.")

            if previous_obj._meta.app_label != self.current_obj._meta.app_label:
                raise Exception("current_obj and previous_obj must be from "
                                "the same Django app.")

            if previous_obj._meta.object_name != self.current_obj._meta.object_name:
                raise Exception("current_obj and previous_obj must be "
                                "instances of the same Django model.")

            self.previous_obj = previous_obj

        if user:
            self.user = user

        if extras:
            if not isinstance(extras, list):
                raise TypeError("extras must be a list.")

            for extra in extras:
                if len(extra.split("__")) > 1:
                    steps = extra.split("__")[:-1]
                    obj = self.current_obj

                    for step in steps:
                        if not hasattr(obj, step):
                            raise Exception(
                                "'%s' in %s is not a valid attribute." % (
                                    step, extra))

                        if not isinstance(getattr(obj, step), Model):
                            raise Exception(
                                "'{0}' in {1} is not a subclass of "
                                "django.db.models.Model.".format(step, extra))

                        obj = getattr(obj, step)
                else:
                    if not hasattr(self.current_obj, extra):
                        raise Exception(
                            "The attribute '{0}' does not exist on the "
                            "current instance.".format(extra))

            self.extras = extras

    def _create_extra_logs(self, log):
        for field in self.extras:
            obj = self.current_obj

            if len(field.split("__")) > 1:
                steps = field.split("__")
                field_name = steps.pop(-1)

                for step in steps:
                    obj = getattr(obj, step)
            else:
                field_name = field

            LogExtra.objects.create(
                log=log,
                field_name=field_name,
                field_value=getattr(obj, field_name)
            )

    def create(self):
        model = type(self.current_obj)
        # Limit logging to editable fields
        fields = [f.name for f in model._meta.get_fields() if f.editable]

        log = Log(
            action=self.action,
            app_name=self.current_obj._meta.app_label,
            model_name=self.current_obj._meta.object_name,
            model_instance_pk=self.current_obj.pk,
            current_json_blob=serializers.serialize("json", [self.current_obj],
                                                    fields=fields)
        )

        if self.previous_obj:
            log.previous_json_blob = serializers.serialize(
                "json", [self.previous_obj], fields=fields)

        if self.user:
            log.user_id = self.user.pk

        # No changes - discard this log attempt
        if self.previous_obj and log.previous_json_blob == log.current_json_blob:
            return None

        # Unique identifier for a model instance
        log_key = "{0.app_name}-{0.model_name}-{0.model_instance_pk}".format(log)
        try:
            log_history_storage = log_history.get()
        except LookupError:
            # No log sequence context was started - do not squash logs
            log_history_storage = None
            squashed_log = None
        else:
            # A log history sequence exists - squash them if possible
            squashed_log, updated_log_ids = self.squash_log_sequence(
                log, log_history_storage.get(log_key, []))
            log_history_storage[log_key] = updated_log_ids

        if squashed_log:
            # Store the new log sequence
            log_history.set(log_history_storage)
            return squashed_log
        else:
            log.save()
            if log_history_storage is not None:
                log_history_storage[log_key].append(log.id)
                # Store the new log sequence
                log_history.set(log_history_storage)

            if self.extras:
                self._create_extra_logs(log)
            return log

    @classmethod
    def squash_log_sequence(cls, log, prev_log_ids):
        """Given a non-persistant log & the ids of existing logs in the sequence, squash sequential
           logs into a resultant log.  Returns the updated log, and the updated list of log id's"""
        squashed_log = None
        updated_log_ids = prev_log_ids

        # Get previous logs of the same model instance
        if prev_logs := Log.objects.filter(id__in=prev_log_ids).order_by("-timestamp"):
            # A DELETE nullifies previous logs in the sequence - remove them.
            if log.action == ACTION_DELETE:
                prev_logs.delete()
                updated_log_ids = []

            # An UPDATE log can squash its changes onto previous update logs to the same
            # object. For example - a CREATE, followed by 3 UPDATES, would squash down to a single
            # CREATE.
            elif log.action == ACTION_UPDATE:
                current_log = log
                # Logs to delete after combining
                to_delete = []
                for prev_log in prev_logs:
                    # Sanity check
                    if prev_log.action == ACTION_DELETE or current_log.action == ACTION_CREATE:
                        # This should be impossible
                        raise AssertionError(f"Previous log {prev_log.id}: {prev_log.action}, "
                                             f"Current log {current_log.id} {current_log.action}")
                    curr_log_dict = json.loads(current_log.current_json_blob)
                    prev_log_dict = json.loads(prev_log.current_json_blob)
                    # Update prev log with this log's changes
                    prev_log_dict[0]["fields"].update(curr_log_dict[0]["fields"])
                    if current_log.id:
                        to_delete.append(current_log.id)
                    current_log = prev_log
                current_log.save()
                squashed_log = current_log
                # Delete redundant logs
                if to_delete:
                    prev_logs.filter(pk__in=to_delete).delete()
                updated_log_ids = [pk for pk in prev_log_ids if pk not in to_delete]

        return squashed_log, updated_log_ids


    @classmethod
    def create_manual_extra(cls, log_id, field_name, field_value):
        """
        Allows you to manually create a log extra. This is useful in
        situations where you are dealing with GenericForeignKeys.

        * log_id = primary key of the log you will link to.
        * field_name = The name of the field you are linking to.
        * field_value = The value, usually a primary key of the object you
                        wish to reference.
        """
        log = Log.objects.get(pk=log_id)

        extra = LogExtra.objects.create(
            log_id=log.pk, field_name=field_name, field_value=field_value)
        return extra
