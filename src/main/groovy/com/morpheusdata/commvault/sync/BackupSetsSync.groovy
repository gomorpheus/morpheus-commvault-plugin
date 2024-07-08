package com.morpheusdata.commvault.sync

import com.morpheusdata.commvault.CommvaultPlugin
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class BackupSetsSync {
    private CommvaultPlugin plugin
    private MorpheusContext morpheusContext
    private BackupProvider backupProviderModel

    BackupSetsSync(BackupProvider backupProviderModel, CommvaultPlugin plugin) {
        this.backupProviderModel = backupProviderModel
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            log.debug("BackupSetsSync execute")
            Map authConfig = plugin.getAuthConfig(backupProviderModel)

            def clients = morpheusContext.async.referenceData.list(new DataQuery()
                    .withFilter('account.id', backupProviderModel.account.id)
                    .withFilter('category', "${backupProviderModel.type.code}.backup.backupServer.${backupProviderModel.id}")
            ).toList().blockingGet()

            clients.each { client ->
                def listResults = CommvaultBackupUtility.listBackupSets(authConfig, client)
                if(listResults.success) {
                    ArrayList<Map> cloudItems = listResults.backupSets
                    def objCategory = "${backupProviderModel.type.code}.backup.backupSet.${backupProviderModel.id}.${client.id}"
                    Observable<ReferenceDataSyncProjection> existingItems = morpheusContext.async.referenceData.listIdentityProjections(new DataQuery()
                            .withFilter('account.id', backupProviderModel.account.id)
                            .withFilter('category', objCategory)
                    )
                    SyncTask<ReferenceDataSyncProjection, ArrayList<Map>, ReferenceData> syncTask = new SyncTask<>(existingItems, cloudItems)

                    syncTask.addMatchFunction { ReferenceDataSyncProjection domainObject, Map cloudItem ->
                        domainObject.externalId.toString() == cloudItem.externalId.toString()
                    }.onDelete {  removeItems ->
                        deleteBackupSets(removeItems)
                    }.onUpdate {  updateItems ->
                        updateMatchedBackupSets(updateItems)
                    }.onAdd { itemsToAdd ->
                        addMissingBackupSets(itemsToAdd, objCategory, client)
                    }.withLoadObjectDetailsFromFinder {  updateItems ->
                        return morpheusContext.async.referenceData.list( new DataQuery().withFilter('id', 'in', updateItems.collect { it.existingItem.id }))
                    }.start()
                } else {
                    log.error("Error listing backupSets")
                    return Single.just(false).toObservable()
                }
            }
        } catch(Exception ex) {
            log.error("BackupSets Sync error: {}", ex, ex)
        }
    }

    private deleteBackupSets(removeItems) {
        log.debug "deleteBackupSets: ${removeItems}"
        morpheusContext.async.referenceData.remove(removeItems).subscribe().dispose()
    }

    private updateMatchedBackupSets(updateItems) {
        log.debug "updateMatchedBackupSets: ${updateItems}"
        updateItems.each {updateMap ->

            Boolean doSave = false
            if(updateMap.existingItem.name != updateMap.masterItem.name) {
                updateMap.existingItem.name = updateMap.masterItem.name
                doSave = true
            }

            if (doSave == true) {
                log.debug "updating backupSets!! ${updateMap.existingItem.name}"
                morpheusContext.async.referenceData.save(updateMap.existingItem).blockingGet()
            }
        }
    }

    private addMissingBackupSets(itemsToAdd, objCategory, client) {
        log.debug "addMissingBackupSets: ${itemsToAdd}"

        def adds = []
        itemsToAdd.each { Map cloudItem ->
            def addConfig = [
                    account         : backupProviderModel.account,
                    code            : objCategory + '.' + cloudItem.externalId,
                    category        : objCategory,
                    name            : cloudItem.name,
                    keyValue        : cloudItem.externalId,
                    value           : cloudItem.externalId,
                    internalId      : cloudItem.internalId,
                    externalId      : cloudItem.externalId,
                    refType         : "ReferenceData",
                    refId           : client.id
            ]

            def add = new ReferenceData(addConfig)
            add.setConfigMap(cloudItem)
            adds << add
        }

        if(adds.size() > 0) {
            log.debug "adding backup sets: ${adds}"
            def success = morpheusContext.async.referenceData.create(adds).blockingGet()
            if(!success) {
                log.error "Error adding backupSets"
            }
        }
    }
}
