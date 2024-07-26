/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.morpheusdata.commvault

import com.morpheusdata.commvault.backup.CommvaultBackupProvider
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.AccountCredential
import com.morpheusdata.model.BackupProvider

class CommvaultPlugin extends Plugin {

    static apiBasePath = '/SearchSvc/CVWebService.svc'

    @Override
    String getCode() {
        return 'commvault'
    }

    @Override
    void initialize() {
        this.setName("Commvault")
        this.registerProvider(new CommvaultBackupProvider(this,this.morpheus))
    }

    /**
     * Called when a plugin is being removed from the plugin manager (aka Uninstalled)
     */
    @Override
    void onDestroy() {
        //nothing to do for now
    }

    def getAuthConfig(BackupProvider backupProvider) {
        //credentials
        backupProvider = loadCredentials(backupProvider)
        def rtn = [
                apiUrl: CommvaultBackupUtility.getApiUrl(backupProvider),
                username:backupProvider.credentialData?.username ?: backupProvider.username,
                password:backupProvider.credentialData?.password ?: backupProvider.password,
                basePath:apiBasePath
        ]
        return rtn
    }

    MorpheusContext getMorpheusContext() {
        this.morpheus
    }

    BackupProvider loadCredentials(BackupProvider backupProvider) {
        if(!backupProvider.credentialLoaded) {
            AccountCredential accountCredential
            accountCredential = this.morpheus.services.accountCredential.loadCredentials(backupProvider)
            backupProvider.credentialLoaded = true
            backupProvider.credentialData = accountCredential?.data
        }
        return backupProvider
    }
}
