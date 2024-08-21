try:
    import json
except ImportError:
    from django.utils import simplejson as json

from django.apps import apps
from django.db import models
from django.contrib.auth.models import User

from .constants import ACTION_TO_STRING


class Log(models.Model):
    """ Log model """
    action = models.SmallIntegerField(db_index=True)
    app_name = models.CharField(
        blank=True,
        db_index=True,
        default='',
        max_length=255
    )
    model_name = models.CharField(
        blank=True,
        db_index=True,
        default='',
        max_length=255
    )
    model_instance_pk = models.CharField(
        blank=True,
        db_index=True,
        default='',
        max_length=255
    )
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    previous_json_blob = models.TextField(blank=True, default="")
    current_json_blob = models.TextField(blank=True, default="")
    user_id = models.IntegerField(blank=True, null=True)

    class Meta:
        ordering = ["-timestamp"]

    def __str__(self):
        return " | ".join([str(p) for p in (
            self.action_name,
            self.app_name,
            self.model_name,
            self.timestamp)
            if p
        ])

    @property
    def django_user(self):
        """ Try to return a standard Django user. """
        try:
            return User.objects.get(pk=self.user_id)
        except User.DoesNotExist:
            return None

    @property
    def get_current_json_blob(self):
        """ Return json string of current blob. """
        return json.dumps(self.current_json_blob)

    @property
    def get_previous_json_blob(self):
        """ Return json string of previous blob. """
        if self.previous_json_blob:
            return json.dumps(self.previous_json_blob)
        return None

    @property
    def current_obj_dict(self):
        obj_dict = json.loads(self.current_json_blob)
        # Log json has been surrounded by a [], for some reason
        if isinstance(obj_dict, list) and len(obj_dict):
            obj_dict = obj_dict[0]
        return obj_dict

    @property
    def previous_obj_dict(self):
        if not self.previous_json_blob:
            return None

        obj_dict = json.loads(self.previous_json_blob)
        # Log json has been surrounded by a [], for some reason
        if isinstance(obj_dict, list) and len(obj_dict):
            obj_dict = obj_dict[0]
        return obj_dict

    @property
    def current_obj_fields(self):
        """ Returns a dict of fields """
        return self.current_obj_dict["fields"]

    @property
    def previous_obj_fields(self):
        """ Returns a dict of fields """
        return self.previous_obj_dict["fields"]

    def get_model(self):
        """ Return the log subject's model """
        try:
            return apps.get_model(app_label=self.app_name, model_name=self.model_name)
        except LookupError:
            return None

    def get_model_instance(self):
        """ Returns the log subject's model instance """
        if model := self.get_model():
            try:
                return model.objects.get(pk=self.model_instance_pk)
            except model.DoesNotExist:
                pass
        return None

    @property
    def action_name(self):
        return ACTION_TO_STRING[self.action]


class LogExtra(models.Model):
    """
    Log Extra model which is used for attaching extra filterable
    data to log objects.
    """
    log = models.ForeignKey(
        Log,
        related_name="extras",
        on_delete=models.CASCADE
    )
    field_name = models.CharField(db_index=True, max_length=255)
    field_value = models.CharField(db_index=True, max_length=255)

    class Meta:
        ordering = ["-log__timestamp"]

    def __str__(self):
        return self.log.__str__()
