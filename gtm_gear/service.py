import os
import json
from google.oauth2 import service_account
import googleapiclient.discovery
import google.auth

from ratelimit import limits, sleep_and_retry

from .cache import Cache

import logging
logger = logging.getLogger(__name__)

REQUESTS_PER_PERIOD = 50
REQUESTS_PERIOD = 60
SLEEP_TIME_DEFAULT = 6

class Service:
    def __init__(self, credentials=None, cache_prefix='', http=None, requestBuilder=None):
        if "GTM_API_CONFIG_FOLDER" in os.environ:
            self.config_folder = os.environ["GTM_API_CONFIG_FOLDER"]
        else:
            self.config_folder = "./"

        self.cache_prefix = cache_prefix
        self.api_name = "tagmanager"
        self.api_version = "v2"
        self.scope = [
            "https://www.googleapis.com/auth/tagmanager.edit.containers"
        ]

        self.requests = REQUESTS_PER_PERIOD
        self.period = REQUESTS_PERIOD        
        self.execute = sleep_and_retry(limits(calls=self.requests, period=self.period)(self.execute_method))

        # Check if credentials are provided
        if credentials is None:
            # Attempt to use Application Default Credentials (ADC)
            credentials, project = google.auth.default(scopes=self.scope)
        else:
            if isinstance(credentials, dict):
                if "service_account_file" in credentials:
                    credentials = service_account.Credentials.from_service_account_file(
                        credentials["service_account_file"], scopes=self.scope)
                else:
                    # Load credentials from the session.
                    credentials = google.oauth2.credentials.Credentials(
                        **credentials
                    )

        self.gtmservice = googleapiclient.discovery.build(
            self.api_name, self.api_version, credentials=credentials
        )

        self.cache = Cache(self.config_folder, self.cache_prefix)

    def getService(self):
        # The function is now simplified and doesn't need to handle OAuth flow directly
        return self.gtmservice

    def get_cache(self, entity, cache=True):
        return self.cache.get_cache(entity, cache)

    def set_ratelimit(self, requests, period):
        self.requests = requests
        self.period = period
        self.execute = sleep_and_retry(limits(calls=self.requests, period=self.period)(self.execute_method))

    def get_accounts(self, cache=True):
        def requests_accounts(service):
            result = (
                self.execute(service.gtmservice.accounts().list())
            )
            return result

        def get_entities():
            return requests_accounts(self)

        result = self.get_cache(
            {
                "path": '',
                "type": 'account',
                "get": get_entities
            }, cache
        )
        return result

    def get_permissions(self, account_id):
        account_path = "accounts/{}".format(account_id)
        result = (
            self.execute(self.gtmservice.accounts().user_permissions().list(parent=account_path))
        )
        return result

    def get_containers(self, account_id, cache=True):
        def requests_containers(service, account_id):
            account_path = "accounts/{}".format(account_id)
            result = (
                service.execute(service.gtmservice.accounts()
                                .containers()
                                .list(parent=account_path)
                )
            )
            return result

        def get_entities():
            return requests_containers(self, account_id)

        result = self.get_cache(
            {
                "path": str(account_id),
                "type": 'container',
                "get": get_entities
            }, cache
        )
        return result

    def is_workspace_changed(self, workspace, entity_path, cache=True):
        cache_file_path = self.cache.get_cache_file_path('workspace', entity_path)
        cache_file_folder = self.cache.get_cache_file_folder(entity_path)

        workspace_cache = self.cache.get_cache_file(cache_file_path)
        if workspace_cache and workspace_cache['fingerprint'] == workspace['fingerprint']:
            return True
        if cache:
            self.cache.save(cache_file_folder, cache_file_path, workspace)        
        
        return False

    def update_cache(self, entity_type, entity_path, data):
        self.cache.update_cache(entity_type, entity_path, data)

    def execute_method(self, object):
        return object.execute()
