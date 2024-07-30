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
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.response.ServiceResponse

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

    def captureActiveSubclientBackup(authConfig, subclientId, clientId, backupsetId) {
        def activeJobsResults = CommvaultBackupUtility.getBackupJobs(authConfig, subclientId, [query: [clientId: clientId, backupsetId: backupsetId, jobFilter: "Backup", jobCategory: "Active"]])
        if(activeJobsResults.success && activeJobsResults.results.size() > 0) {
            def activeBackupJob = activeJobsResults.results.find { it.clientId == clientId && it.subclientId == subclientId && it.backupsetId == backupsetId }
            return ServiceResponse.success([backupJobId: activeBackupJob.backupJobId])
        }
    }

    def getBackupStatus(backupState) {
        def status
        if(backupState.toLowerCase().contains("completed") && backupState.toLowerCase().contains("errors")) {
            status = BackupResult.Status.SUCCEEDED_WARNING
        } else if(backupState.contains("Failed") || backupState.contains("errors")) {
            status = BackupResult.Status.FAILED
        } else if(["Interrupted", "Killed", "Suspend", "Suspend Pending", "Kill Pending"].contains(backupState) || backupState.contains("Killed")) {
            status = BackupResult.Status.CANCELLED
        } else if(["Running", "Waiting", "Pending"].contains(backupState) || backupState.contains("Running")) {
            status = BackupResult.Status.IN_PROGRESS
        } else if(backupState == "Completed" || backupState.contains("Completed")) {
            status = BackupResult.Status.SUCCEEDED
        } else if(backupState == "Queued") {
            status = BackupResult.Status.START_REQUESTED
        } else if(["Kill", "Pending" ,"Interrupt", "Pending"].contains(backupState)) {
            status = BackupResult.Status.CANCEL_REQUESTED
        }

        return status ? status.toString() : status
    }
}
