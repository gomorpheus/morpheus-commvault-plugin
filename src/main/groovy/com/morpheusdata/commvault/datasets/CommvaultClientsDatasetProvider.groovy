package com.morpheusdata.commvault.datasets

import com.morpheusdata.commvault.utils.CommvaultComputeUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.*
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.ResourcePermission
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class CommvaultClientsDatasetProvider extends AbstractDatasetProvider<ReferenceData, String> {

    public static final providerName = 'Commvault Clients Dataset Provider'
    public static final providerNamespace = 'commvault'
    public static final providerKey = 'commvaultClients'
    public static final providerDescription = 'A collection of key/value pairs' // need to change

    CommvaultClientsDatasetProvider(Plugin plugin, MorpheusContext morpheus) {
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
        return null
    }

    /**
     * list the values in teh dataset in a common format of a name value pair. (example: [[name: "blue", value: 1]])
     * @param query a DatasetQuery containing the user and map of query params or options to apply to the list
     * @return a list of maps that have name value pairs of the items
     */
    @Override
    Observable<Map> listOptions(DatasetQuery query) {
        log.debug("clients: ${query.parameters}")
        List clients = []
        Long cloudId = query.get("zoneId")?.toLong()
        def account = query.user.account
        Cloud cloud = null
        def backupProvider
        if (cloudId) {
            cloud = morpheus.async.cloud.get(cloudId).blockingGet()
        }
        if (cloud && cloud.backupProvider) {
            backupProvider = morpheus.services.backupProvider.find(new DataQuery().withFilter("account", account).withFilter("id", cloud.backupProvider.id))
        }
        if (backupProvider) {
            def accessibleResourceIds = morpheus.services.resourcePermission.listAccessibleResources(account.id, ResourcePermission.ResourceType.NetworkServer, null, null)
            // dustin will add in enum class
            // replace NetworkServer with BackupServer
            def dataQuery = new DataQuery().withFilters([
                    new DataFilter("account", backupProvider.account),
                    new DataFilter("category", "${backupProvider.type.code}.backup.backupServer.${backupProvider.id}"),
                    new DataFilter("enabled", true)
            ])
            def dataOrFilter = new DataOrFilter(
                    new DataFilter("account", account),
                    new DataAndFilter(
                            new DataFilter("account.masterAccount", account.masterAccount),
                            new DataFilter("visibility", "public")
                    )
            )
            if (accessibleResourceIds) {
                dataOrFilter.withFilter(new DataFilter("id", "in", accessibleResourceIds))
            }
            dataQuery.withFilter(dataOrFilter)
            def clientResults = morpheus.services.referenceData.list(dataQuery)
            if (clientResults.size() > 0) {
                clientResults.each { client ->
                    def clientInstanceType = client.getConfigProperty('vsInstanceType')
                    def clientInstanceTypeCode = clientInstanceType ? CommvaultComputeUtility.getvsInstanceType(clientInstanceType?.toString()) : null
                    if (!cloud || cloud?.cloudType?.provisionTypes.find { it.code == clientInstanceTypeCode }) {
                        clients << [name: client.name, id: client.id, value: client.id]
                    }
                }
            } else {
                clients << [name: "No clients setup in Commvault", id: '']
            }
        } else {
            clients << [name: "No Commvault backup provider found.", id: '']
        }
        return clients
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
        return null
    }

    /**
     * returns the item from the list with the matching value
     * @param value the value to match the item in the list
     * @return the
     */
    @Override
    ReferenceData item(String value) {
        return null
    }

    /**
     * gets the name for an item
     * @param item an item
     * @return the corresponding name for the name/value pair list
     */
    @Override
    String itemName(ReferenceData item) {
        return null
    }

    /**
     * gets the value for an item
     * @param item an item
     * @return the corresponding value for the name/value pair list
     */
    @Override
    String itemValue(ReferenceData item) {
        return null
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
