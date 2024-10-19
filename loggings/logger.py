from copy import deepcopy
from collections import defaultdict
import contextvars
import json

from django.db.models import Model

from .constants import ACTION_CREATE, ACTION_DELETE, ACTION_UPDATE
from .helpers import create_extra, normalize_extras, serialize_obj
from .models import Log


# Stored log id's per model instance, for use with squashing a log sequence
_log_sequence_context = contextvars.ContextVar("loggings___log_sequence")


def begin_log_sequence():
    """Begin recording log id's per model instance."""
    return _log_sequence_context.set(defaultdict(list))


def end_log_sequence(token):
    """End recording log id's per model instance."""
    _log_sequence_context.reset(token)


class Logger(object):
    actions = (ACTION_CREATE, ACTION_UPDATE, ACTION_DELETE)
    extras = None
    previous_obj = None
    user = None

    def __init__(self, action, current_obj, previous_obj=None, user=None,
                 current_obj_json=None, previous_obj_json=None, extras=None, manual_extras=None):

        try:
            action = int(action)
        except ValueError:
            raise ValueError("Action must be an integer.")

        if action not in self.actions:
            raise Exception(f"Action must be an integer in {self.actions}")
        self.action = action

        if not isinstance(current_obj, Model):
            raise TypeError("current_obj must be a Django model instance.")

        # Use a fresh db copy, so that fields will be pre-converted (to_python())
        # This will minimize false diffs when logging
        self.current_obj = deepcopy(current_obj)
        self.current_obj.refresh_from_db()
        self.current_obj_json = current_obj_json

        self.previous_obj_json = previous_obj_json
        if previous_obj:
            if not isinstance(previous_obj, type(self.current_obj)):
                raise TypeError("current_obj and previous_obj must be instances of the same "
                                "Django model.")
            self.previous_obj = previous_obj

        self.user = user
        self.extras = normalize_extras(current_obj, extras, manual_extras)

    def _create_extra_logs(self, log):
        for field_name, value in self.extras:
            create_extra(log.id, field_name, value)

    def create(self):
        model = type(self.current_obj)
        # Limit logging to editable fields
        fields = [f.name for f in model._meta.get_fields() if f.editable]

        log = Log(
            action=self.action,
            app_name=self.current_obj._meta.app_label,
            model_name=self.current_obj._meta.object_name,
            model_instance_pk=self.current_obj.pk,
            current_json_blob=(
                self.current_obj_json or serialize_obj(self.current_obj, fields=fields))
        )

        if self.previous_obj:
            log.previous_json_blob = (self.previous_obj_json
                                      or serialize_obj(self.previous_obj, fields=fields))

        if self.user:
            log.user_id = self.user.pk

        # No changes - discard this log attempt
        if self.previous_obj and log.previous_json_blob == log.current_json_blob:
            return None

        # Unique identifier for a model instance
        log_key = "{0.app_name}-{0.model_name}-{0.model_instance_pk}".format(log)
        try:
            log_sequences = _log_sequence_context.get()
        except LookupError:
            # No log sequence context was started - do not squash logs
            log_sequences = None
        else:
            # A log history sequence exists - squash them if possible
            log, updated_log_ids = self.squash_log_sequence(
                log, log_sequences.get(log_key, []), extras=self.extras)
            log_sequences[log_key] = updated_log_ids

        # Save log & append to history
        if log:
            created = not log.pk
            log.save()
            self._create_extra_logs(log)

            if created and log_sequences is not None:
                log_sequences[log_key].append(log.id)

        # Store the new log sequence
        if log_sequences is not None:
            _log_sequence_context.set(log_sequences)

        return log


    @classmethod
    def squash_log_sequence(cls, log, prev_log_ids, extras=None):
        """Given a non-persistant log & the ids of existing logs in the sequence, squash sequential
           logs into a resultant log.  Returns the updated log, and the updated list of log id's"""
        # The final, squashed log
        resultant_log = log
        # Logs to delete after combining
        to_delete = set()
        # The new log sequence
        updated_log_ids = prev_log_ids

        # Get previous logs of the same model instance
        if prev_logs := Log.objects.filter(id__in=prev_log_ids).order_by("-timestamp"):
            # A DELETE nullifies previous logs in the sequence - remove them.
            if log.is_delete:
                for prev_log in prev_logs:
                    # Only squash if logs have the same user_id
                    if prev_log.user_id == log.user_id:
                        to_delete.add(prev_log.id)
                    else:
                        break

            # An UPDATE log can squash its changes onto previous update logs to the same
            # object. For example - a CREATE, followed by 3 UPDATES, would squash down to a single
            # CREATE.
            elif log.is_update:
                # Readable alias for iterating
                current_log = log

                for prev_log in prev_logs:
                    # Sanity check! There should never exist a log:
                    #   - before CREATE (the birth of an obj)
                    #   - after DELETE  (the death of an obj)
                    if current_log.is_create or prev_log.is_delete:
                        raise AssertionError(
                            f"Previous log {prev_log.id}: {prev_log.action_name}, "
                            f"Current log {current_log.id} {current_log.action_name}")

                    # Only squash logs with the same user_id
                    if prev_log.user_id != log.user_id:
                        break

                    # Only squash logs with the same extras
                    prev_extras = set([
                        f"{ex.field_name} {ex.field_value}" for ex in prev_log.extras.all()])
                    current_extras = set([f"{field} {value}" for field, value in (extras or [])])
                    if prev_extras.difference(current_extras):
                        break

                    # Extract changes
                    curr_log_dict = current_log.current_obj_dict
                    prev_log_dict = prev_log.current_obj_dict

                    # Squash current log onto prev log
                    prev_log_dict["fields"].update(curr_log_dict["fields"])
                    prev_log.current_json_blob = json.dumps(prev_log_dict)

                    # Record current log for deletion
                    if current_log.id:
                        to_delete.add(current_log.id)

                    # Keep reference to the previous item
                    current_log = resultant_log = prev_log

            # Squashing 2 logs may cancel each other's changes, such as flipping a bool, twice
            if resultant_log.current_json_blob == resultant_log.previous_json_blob:
                if resultant_log.pk:
                    to_delete.add(resultant_log.pk)
                resultant_log = None

        # Delete redundant logs
        if to_delete:
            Log.objects.filter(pk__in=to_delete).delete()
            updated_log_ids = [pk for pk in prev_log_ids if pk not in to_delete]
        return resultant_log, updated_log_ids


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
        return create_extra(log_id, field_name, field_value)
