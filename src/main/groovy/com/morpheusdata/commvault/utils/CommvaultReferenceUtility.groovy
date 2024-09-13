package com.morpheusdata.commvault.utils

import com.morpheusdata.model.BackupResult

/**
 * @author rahul.ray
 */
class CommvaultReferenceUtility {

    static String getvsInstanceType(String clientInstanceType) {
        def instanceTypeMapping = [
                '0': "none",
                '1': "vmware",
                '2': "hyperv",
                '3': "xen",
                '4': "amazon",
                '5': "azure",
                '6': "Red Hat Enterprise Virtualization (RHEV)",
                '7': "Microsoft Azure Resource Manager",
                '9': "nutanix",
                '10': "oraclevm",
                '11': "docker",
                '12': "openstack",
                '13': "oracle",
                '14': "huawei",
                '16': "google",
                '17': "Microsoft Azure Stack",
                '19': "oraclecloud"
        ]
        return instanceTypeMapping[clientInstanceType]
    }

    static getBackupStatus(backupState) {
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
