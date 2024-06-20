package com.morpheusdata.commvault.datasets

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.ReferenceData
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */
@Slf4j
class CommvaultBackupSetsDatasetProvider extends AbstractDatasetProvider<ReferenceData, Long> {

    public static final providerName = 'Commvault BackupSets Dataset Provider'
    public static final providerNamespace = 'commvault'
    public static final providerKey = 'commvaultBackupSets'
    public static final providerDescription = 'A set of data from commvault BackupSets'

    CommvaultBackupSetsDatasetProvider(Plugin plugin, MorpheusContext morpheus) {
        this.plugin = plugin
        this.morpheusContext = morpheus
    }

    @Override
    DatasetInfo getInfo() {
        new DatasetInfo(
                name: providerName,
                namespace: providerNamespace,
                key: providerKey,
                description: providerDescription
        )
    }
    /**
     * the class type of the data for this provider
     * @return the class this provider operates on
     */
    @Override
    Class<ReferenceData> getItemType() {
        return null
    }

    /**
     * list the values in the dataset
     * @param query the user and map of query params or options to apply to the list
     * @return a list of objects
     */
    @Override
    Observable<ReferenceData> list(DatasetQuery query) {
        def account = query.user.account
        def clientId = query.get("clientId")
        def cloud = null
        def backupProvider
        Long containerId = query.get("containerId")?.toLong()
        Long cloudId = query.get("zoneId")?.toLong()
        if (containerId && !cloudId) {
            def workload = morpheusContext.services.workload.find(new DataQuery().withFilter("account", account).withFilter("containerId", containerId))
            cloud = workload?.server?.cloud
        }
        if (!cloud && cloudId) {
            cloud = morpheus.async.cloud.get(cloudId).blockingGet()
        }
        if (cloud?.backupProvider) {
            backupProvider = morpheus.services.backupProvider.find(new DataQuery().withFilter("account", account).withFilter("id", cloud.backupProvider.id))
        }
        if (backupProvider && clientId) {
            def refData = morpheusContext.services.referenceData.list(new DataQuery()
                    .withFilter("category", "${backupProvider.type.code}.backup.backupSet.${backupProvider.id}.${clientId}")
                    .withFilter("refType", "ReferenceData").withFilter("refId", "clientId"))
            Observable.fromIterable(refData)
        }
        return Observable.empty()
    }

    /**
     * list the values in teh dataset in a common format of a name value pair. (example: [[name: "blue", value: 1]])
     * @param query a DatasetQuery containing the user and map of query params or options to apply to the list
     * @return a list of maps that have name value pairs of the items
     */
    @Override
    Observable<Map> listOptions(DatasetQuery query) {
        def backupSetResults = list(query).toList().blockingGet().collect { refData ->
            [name: refData.name, id: refData.id, value: refData.id]
        }
        def backupSets = backupSetResults.isEmpty() ? [[name: "No backup Sets setup in Commvault", id: '']] : backupSetResults
        return Observable.fromIterable(backupSets)
    }

    /**
     * returns the matching item from the list with the value as a string or object - since option values
     *   are often stored or passed as strings or unknown types. lets the provider do its own conversions to call
     *   item with the proper type. did object for flexibility but probably is usually a string
     * @param value the value to match the item in the list
     * @return the item
     */
    @Override
    ReferenceData fetchItem(Object value) {
        def rtn = null
        if (value instanceof Long) {
            rtn = item((Long) value)
        } else if (value instanceof CharSequence) {
            def longValue = value.isNumber() ? value.toLong() : null
            if (longValue) {
                rtn = item(longValue)
            }
        }
        return rtn
    }

    /**
     * returns the item from the list with the matching value
     * @param value the value to match the item in the list
     * @return the
     */
    @Override
    ReferenceData item(Long value) {
        return morpheus.services.referenceData.get(value)
    }

    /**
     * gets the name for an item
     * @param item an item
     * @return the corresponding name for the name/value pair list
     */
    @Override
    String itemName(ReferenceData item) {
        return item.name
    }

    /**
     * gets the value for an item
     * @param item an item
     * @return the corresponding value for the name/value pair list
     */
    @Override
    Long itemValue(ReferenceData item) {
        return item.id
    }

    /**
     * Returns true if the Provider is a plugin. Always true for plugin but null or false for Morpheus internal providers.
     * @return provider is plugin
     */
    @Override
    boolean isPlugin() {
        return true
    }
}
