package com.morpheusdata.commvault.sync

import com.morpheusdata.commvault.utils.CommvaultApiUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import groovy.util.logging.Slf4j

/**
 * @author rahul.ray
 */

@Slf4j
class StoragePoliciesSync {

    MorpheusContext morpheus
    BackupProvider backupProvider
    Map authConfigMap

    StoragePoliciesSync(MorpheusContext morpheus, BackupProvider backupProvider, Map authConfigMap) {
        this.morpheus = morpheus
        this.backupProvider = backupProvider
        this.authConfigMap = authConfigMap
    }

    def execute() {
        log.debug("StoragePoliciesSync >> execute()")
        try {
            def listResults = CommvaultApiUtility.listStoragePolicies(this.authConfigMap)
            log.debug("listResults.success: ${listResults.success}")
            if (listResults.success) {
                def objList = listResults.storagePolicies
                objList.each { storagePolicy ->
                    def policyCopy = CommvaultApiUtility.getStoragePolicyCopy(this.authConfigMap, storagePolicy.externalId)
                    storagePolicy.copyId = policyCopy.result?.externalId
                    storagePolicy.copyName = policyCopy.result?.name
                }
                def existingItems = morpheus.async.referenceData.listIdentityProjections(
                        new DataQuery().withFilters([new DataFilter('account.id', backupProvider.account.id),
                                                     new DataFilter('category', "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}")])
                )
                SyncTask<ReferenceDataSyncProjection, Map, ReferenceData> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                syncTask.addMatchFunction { ReferenceDataSyncProjection domainObject, Map cloudItem ->
                    domainObject.externalId.toString() == cloudItem.externalId.toString()
                }.onDelete { removeItems ->
                    log.debug("removeItems:", removeItems.size())
                    morpheus.services.referenceData.bulkRemove(removeItems)
                }.onAdd { itemsToAdd ->
                    addMissingStoragePolicies(itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
                    morpheus.async.referenceData.listById(updateItems?.collect { it.existingItem.id }).map { ReferenceData referenceData ->
                        SyncTask.UpdateItemDto<ReferenceDataSyncProjection, Map> matchItem = updateItemMap[referenceData.id]
                        return new SyncTask.UpdateItem<ReferenceData, Map>(existingItem: referenceData, masterItem: matchItem.masterItem)
                    }
                }.onUpdate { updateList ->
                    updateMatchedStoragePolicies(updateList)
                }.start()
            }
        } catch (e) {
            log.error("StoragePoliciesSync error: ${e}", e)
        }
    }

    private addMissingStoragePolicies(Collection<Map> addList) {
        log.debug("addMissingStoragePolicies: addList.size(): ${addList.size()}")
        def policyAdds = []
        try {
            def objCategory = "${backupProvider.type.code}.backup.storagePolicy.${backupProvider.id}"
            addList?.each { cloudItem ->
                def addConfig = [
                        account   : backupProvider.account,
                        code      : objCategory + '.' + cloudItem.externalId,
                        category  : objCategory,
                        name      : cloudItem.name,
                        keyValue  : cloudItem.externalId,
                        value     : cloudItem.externalId,
                        internalId: cloudItem.internalId,
                        externalId: cloudItem.externalId,
                        type      : 'string'
                ]
                def add = new ReferenceData(addConfig)
                add.setConfigMap(cloudItem)
                policyAdds << add
            }
            if (policyAdds.size() > 0) {
                morpheus.services.referenceData.bulkCreate(policyAdds)
            }
        } catch (e) {
            log.error "Error in addMissingStoragePolicies ${e}", e
        }
    }

    private updateMatchedStoragePolicies(List<SyncTask.UpdateItem<ReferenceData, Map>> updateList) {
        log.debug "updateMatchedStoragePolicies: ${updateList.size()}"
        def saves = []
        def doSave = false
        try {
            for (updateItem in updateList) {
                doSave = false
                def existingItem = updateItem.existingItem
                def masterItem = updateItem.masterItem
                if (existingItem.name != masterItem.name) {
                    existingItem.name = masterItem.name
                    doSave = true
                }
                if (existingItem.getConfigProperty('copyId') != masterItem.copyId) {
                    existingItem.setConfigProperty('copyId', masterItem.copyId)
                    existingItem.setConfigProperty('copyName', masterItem.copyName)
                    doSave = true
                }
                if (doSave == true) {
                    saves << existingItem
                }
            }
            if (saves) {
                morpheus.services.referenceData.bulkSave(saves)
            }
        } catch (e) {
            log.error("updateMatchedStoragePolicies error: ${e}", e)
        }
    }
}