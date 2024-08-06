package com.morpheusdata.commvault.utils

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import groovy.xml.XmlSlurper

@Slf4j
class CommvaultApiUtility {

	// backup servers
	static listClients(authConfig){
		def rtn = [success:true, clients: []]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def query = ['PseudoClientType': 'VSPseudo'] // only list virtualization clients
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Client", authConfig.token, [format:'json', query: query], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			response.VSPseudoClientsList.each { row ->
				def newClient = [
					internalId: row.client.clientId,
					externalId: row.client.clientGUID,
					name: row.client.clientName,
					displayName: row.client.displayName,
					clientId: row.client.clientId,
					clientGUID: row.client.clientGUID,
					clientName: row.client.clientName,
					hostName: row.client.hostName,
					cloudAppsInstanceType: row.client.cloudAppsInstanceType,
					vsInstanceType: row.VSInstanceType
				]

				// get backupset for default app, instance and backupset

				rtn.clients << newClient
			}
		}

		return rtn
	}

	static getApiUrl(BackupProvider backupProvider) {
		def scheme = backupProvider.host.contains("http") ? "" : "http://"
		def apiUrl = "${scheme}${backupProvider.host}:${backupProvider.port}"

		return apiUrl
	}

	// jobs
	static listSubclients(authConfig, opts){
		def rtn = [success:true, subclients: []]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		opts.each { clientReferenceData ->
			def query = ['clientId': "${clientReferenceData.getConfigProperty("internalId")}"]
			def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient", authConfig.token, [format:'json', query: query], 'GET')

			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.json.JsonSlurper().parseText(results.content)
				response.subClientProperties.each { row ->
					def rtnRow = [
						internalId: row.subClientEntity.subclientId,
						externalId: row.subClientEntity.subclientGUID,
						name: row.subClientEntity.subclientName,
						subclientId: row.subClientEntity.subclientId,
						subclientGUID: row.subClientEntity.subclientGUID,
						subclientName: row.subClientEntity.subclientName,
						clientName: row.subClientEntity.clientName,
						clientId: row.subClientEntity.clientId,
						instanceName: row.subClientEntity.instanceName,
						instanceId: row.subClientEntity.instanceId,
						appName: row.subClientEntity.appName,
						applicationId: row.subClientEntity.applicationId,
						backupsetName: row.subClientEntity.backupsetName,
						backupsetId: row.subClientEntity.backupsetId,
						type: row.subClientEntity._type_
					]

					def subclientDetails = callApi(authConfig.apiUrl, "/SearchSvc/CVWebService.svc/Subclient/${row.subClientEntity.subclientId}", authConfig.token, [format:'json'], 'GET')
					def subclientDetailsResults =  new groovy.json.JsonSlurper().parseText(subclientDetails.content).subClientProperties?.getAt(0)
					rtnRow.storagePolicyId = subclientDetailsResults.commonProperties?.storageDevice?.dataBackupStoragePolicy?.storagePolicyId
					rtnRow.storagePolicyName = subclientDetailsResults.commonProperties?.storageDevice?.dataBackupStoragePolicy?.storagePolicyName
					rtnRow.backupsetId = subclientDetailsResults.subClientEntity?.backupsetId
					rtnRow.backupsetName = subclientDetailsResults.subClientEntity?.backupsetName
					rtn.subclients << rtnRow
				}
			}
		}


		return rtn
	}

	//storage
	static listLibraryItems(authConfig, opts){
		def rtn = [success:true, library: []]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Library", authConfig.token, [format:'json'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			response.response.each { row ->
				// get more info on storage, throwing database connection error
				// def detailResults = getLibraryDetails(row.entityInfo.id, authConfig, opts)

				rtn.library << [
					internalId: row.entityInfo.id,
					externalId: row.entityInfo.id,
					name: row.entityInfo.name
				]
			}
		}

		return rtn
	}

	static getLibraryDetails(Long id, Map authConfig, Map opts){
		def rtn = [success:true, library: [:]]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Library/${id}", authConfig.token, [format:'json'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			rtn.library = response
		}

		return rtn
	}

	static listBackupSets(authConfig, opts){
		def rtn = [success:true, backupSets: []]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def query = ['clientId': "${opts.internalId}"]
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Backupset", authConfig.token, [format:'json', query: query], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			response.backupsetProperties.each { row ->
				rtn.backupSets << [
					internalId: row.backupSetEntity.backupsetId,
					externalId: row.backupSetEntity.backupsetId,
					name: row.backupSetEntity.backupsetName,
					clientId: opts.internalId
				] + row.backupSetEntity

			}
		}

		return rtn
	}

	static listStoragePolicies(authConfig){
		def rtn = [success:true, storagePolicies: []]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def requestQuery = [:] //  [propertyLevel: 10] // include storage policy copies
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/StoragePolicy", authConfig.token, [format:'json', query: requestQuery], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			response.policies.each { row ->
				rtn.storagePolicies << [
					internalId: row.storagePolicyId,
					externalId: row.storagePolicyId,
					name: row.storagePolicyName
				]
			}
		}

		return rtn
	}

	//backup job
	static createSubclient(authConfig, client, subclient, Map opts=[:]){
		def rtn = [success: false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def backupSet = opts.backupSet
		def backupSetConfig = backupSet.getConfigMap()
		def storagePolicy = opts.storagePolicy
		def body = [
			subClientProperties: [
				subClientEntity: [
					clientName: client,
					appName: backupSetConfig.appName,
					instanceName: backupSetConfig.instanceName,
					subclientName: subclient
				],
				commonProperties: [
					enableBackup: true
				]
			]
		]
		if(backupSet) {
			body.subClientProperties.subClientEntity.backupsetName = backupSet.name
		}
		if(storagePolicy) {
			body.subClientProperties.commonProperties.storageDevice = [
				dataBackupStoragePolicy: [
					storagePolicyName: storagePolicy.name
				]
			]
		}
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient", authConfig.token, [format:'json', body:body], 'POST')

		if(results?.success == true) {
			def response = results.data
			if(!response.errorCode) {
				def entity = response.response.entity
				rtn.subclientId = entity.subclientId.toString()
				rtn.success = true
			} else {
				rtn.success = false
				rtn.msg = response.errorMessage
				rtn.errorCode = response.errorCode
			}
		}

		return rtn
	}

	static getSubclient(authConfig, subclientId) {
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient/${subclientId}", authConfig.token, [format:'json'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			if(!response.errorCode) {
				rtn.subclient = response.subClientProperties?.getAt(0)
				//rtn.statusCode = results.statusCode
			} else {
				//rtn.statusCode = results.errorCode
				rtn.errorCode = response.errorCode
				rtn.success = false
				rtn.msg = response.errorMessage
			}
		}

		return rtn
	}

	// add backup to job
	static addVMToSubclient(authConfig, subclientId, vmExternalId, vmDisplayName, Map opts=[:]) {
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def requestWriter = new StringWriter()
		def xml = new groovy.xml.MarkupBuilder(requestWriter)
		xml.App_UpdateSubClientPropertiesRequest(){
			subClientProperties(){
				vmContentOperationType("ADD")
				vmContent(){
					children(equalsOrNotEquals: 1, name: vmExternalId, displayName: vmDisplayName, type: "VM")
				}
			}
		}
		def body = requestWriter.toString()
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient/${subclientId}", authConfig.token, [format:'xml', body:body], 'POST')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = results.data
			if(!response.response || response.response.find { it["@errorCode"].toInteger() != 0 }) {
				rtn.success = false
				rtn.msg = "An error occurred adding a VM to the backup job"
			}
		}

		return rtn
	}


	static removeVMFromSubclient(authConfig, subclientId, vmExternalId, vmName, Map opts = [:]) {
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def requestWriter = new StringWriter()
		def xml = new groovy.xml.MarkupBuilder(requestWriter)
		xml.App_UpdateSubClientPropertiesRequest(){
			 subClientProperties(){
				 vmContentOperationType("DELETE")
				 vmContent(){
					 children(equalsOrNotEquals: 1, name: vmExternalId, displayName: vmName, type: "VM")
				 }
			 }
		 }
		def body = requestWriter.toString()
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient/${subclientId}", authConfig.token, [format:'xml', body:body], 'POST')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = results.data
			if(!response.response || response.response.find { it["@errorCode"].toInteger() != 0 }) {
				rtn.success = false
				rtn.msg = "An error occurred removing the VM from the backup job"
				rtn.errorCode = response.errorCode
				rtn.statusCode = results.errorCode
			}
		}

		return rtn
	}

	static deleteSubclient(authConfig, subclientId){
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient/${subclientId}", authConfig.token, [format:'json'], 'DELETE')
		rtn.success = results?.success || results.errorCode == 404
		if(rtn.success) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			if(!response.response || response.response.find { it.errorCode != 0 }) {
				rtn.success = false
				rtn.msg = "An error occurred removing the backup job"
				rtn.errorCode = response.errorCode
				rtn.statusCode = results.errorCode
			}
		}
		return rtn
	}

	static backupSubclient(authConfig, subclientId){
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Subclient/${subclientId}/action/backup", authConfig.token, [format:'json'], 'POST')
		log.debug("got: ${results}")

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			if(response.errorCode && response.errorCode != 0) {
				rtn.success = false
				rtn.msg = response.errorMessage
				rtn.errorCode = response.errorCode
				rtn.statusCode = results.errorCode
			} else {
				rtn.backupJobId = response.jobIds?.getAt(0)
			}
		}

		return rtn
	}

	static restoreVM(authConfig, vmExternalId, backupJob, backupJobId, Map opts=[:]) {
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def requestWriter = new StringWriter()
		def xml = new groovy.xml.MarkupBuilder(requestWriter)
		if(!opts.restoreNew) {
			xml.VMRestoreReq(powerOnVmAfterRestore: true, passUnconditionalOverride:true, inPlaceRestore: true, jobId:backupJobId)
		} else {
			xml.VMRestoreReq(powerOnVmAfterRestore: true, passUnconditionalOverride:false, inPlaceRestore: false, jobId:backupJobId) {
				destinationInfo() {
					vmware(newName:opts.vmName, esxHost:opts.esxHost, dataStore:opts.datastore, resourcePool:opts.resourcePool)
				}
			}
		}

		def body = requestWriter.toString()

		log.debug("Restoring VM, URL: '${authConfig.basePath}/v2/vsa/vm/${vmExternalId}/recover', Request Body: ${body}")
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/v2/vsa/vm/${vmExternalId}/recover", authConfig.token, [format:'xml', body: body], 'POST')

		rtn.success = results?.success
		log.debug("Restore VM Response: ${results}")
		if(rtn.success == true && !results.errorCode) {
			def responseData = results.data
			rtn.restoreJobId = responseData.jobIds["@val"]
		} else {
			rtn.msg = results.errorMessage
			rtn.errorCode = results.errorCode
		}

		return rtn
	}

	static getBackupJobs(authConfig, subclientId, opts = [:]){
		def rtn = [success:true]
		def backupResults = []
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def query = [subclientId:subclientId]
		if(opts.query) {
			query += opts.query
		}
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Job", authConfig.token, [format:'xml', query:query], 'GET')
		log.debug("got: ${results}")

		rtn.success = results?.success
		if(results?.success == true) {
			def response = new XmlSlurper().parseText(results.content)
			response.jobs.jobSummary.each { summary ->
				def subclient = summary.subclient
				def jobId = summary["@jobId"].toString()
				def status = summary["@status"].toString()
				def percentComplete = summary["@percentComplete"].toString()
				def startTime = summary["@jobStartTime"].toString()
				def endTime = summary["@jobEndTime"].toString()
				def applicationId = subclient["@applicationId"]
				def backupsetId = subclient["@backupsetId"]
				def clientId = subclient["@clientId"]
				def instanceId = subclient["@instanceId"]
				def backupResult = [
					backupJobId: jobId, subclientId:subclientId, startTime: startTime, endTime: endTime, state: status,
					result: status, progress: percentComplete, applicationId: applicationId, backupsetId: backupsetId, clientId: clientId,
					instanceId: instanceId
				]
				backupResults << backupResult
			}
		}

		rtn.results = backupResults
		return rtn
	}

	static getAllBackupJobs(authConfig){
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Job", authConfig.token, [format:'xml'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new XmlSlurper().parseText(results.content)
			rtn.jobId = response.jobIds['@val'].toString()
		}

		return rtn
	}

	static getJob(authConfig, backupJobId){
		def rtn = [success:true]
		def jobResult = [:]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Job/${backupJobId}", authConfig.token, [format:'xml'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new XmlSlurper().parseText(results.content)
			def summary = response.jobs.jobSummary

			jobResult = [
				externalId: summary["@jobId"].toString(),
				jobId: summary["@jobId"].toString(),
				subclientId: summary.subclient["@subclientId"].toString(),
				startTime: summary["@jobStartTime"].toString(),
				endTime: summary["@jobEndTime"].toString(),
				state: summary["@status"].toString(),
				result: summary["@status"].toString(),
				status: summary["@status"].toString(),
				progress: summary["@percentComplete"].toString(),
				totalSize: (summary["@sizeOfApplication"].toString().isNumber() ? summary["@sizeOfApplication"] : 0)?.toLong()
			]
		}

		rtn.result = jobResult
		return rtn
	}

	static getVMJobForParentJob(authConfig, vmGuid, parentJobId, Map opts = [:]){
		def rtn = [success:true]
		def result = [:]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def query = [:]
		if(opts.endTime) {
			query.completedJobLookupTime = new Date().toInstant().seconds - opts.endTime.toLong() + 300
		}
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/v2/vsa/vm/${vmGuid}/jobs", authConfig.token, [format:'xml', query: query], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new XmlSlurper().parseText(results.content)
			def job = response.jobs.find { it.jobSummary["@vsaParentJobID"] == parentJobId }

			if(job) {
				result = [
					externalId: job.jobSummary["@jobId"].toString(),
					jobId: job.jobSummary["@jobId"].toString(),
					subclientId: job.jobSummary.subclient["@subclientId"].toString(),
					startTime: job.jobSummary["@jobStartTime"].toString(),
					endTime: job.jobSummary["@jobEndTime"].toString(),
					state: job.jobSummary["@status"].toString(),
					result: job.jobSummary["@status"].toString(),
					status: job.jobSummary["@status"].toString(),
					progress: job.jobSummary["@percentComplete"].toString(),
					totalSize: (job.jobSummary["@sizeOfApplication"].toString().isNumber() ? job.jobSummary["@sizeOfApplication"] : 0)?.toLong()
				]
				if( job.jobSummary["@vsaParentJobID"]) {
					result.parentJobId = job.jobSummary["@vsaParentJobID"].toString()
				}
			}

		}

		rtn.result = result
		return rtn
	}

	static getStoragePolicyCopy(authConfig, storagePolicyId){
		def rtn = [success:true]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/StoragePolicy/${storagePolicyId}", authConfig.token, [format:'json'], 'GET')

		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.json.JsonSlurper().parseText(results.content)
			def item = response.copy.getAt(0)
			if(item) {
				rtn.result = item + [
					externalId: item.StoragePolicyCopy?.copyId,
					name: item.StoragePolicyCopy?.copyName
				]
			}
		}

		return rtn
	}

	static pauseBackupJob(authConfig, backupJobId){
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def taskId = ""
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Job/${backupJobId}/Action/pause", authConfig.token, [format:'xml'], 'POST')
		rtn.success = results?.success

		return rtn
	}

	static killBackupJob(authConfig, backupJobId){
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Job/${backupJobId}/Action/kill", authConfig.token, [format:'xml'], 'POST')
		log.debug("got: ${results}")

		rtn.success = results?.success

		return rtn
	}

	static deleteJob(authConfig, jobId, Map opts=[:]) {
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def body = "qoperation agedata -delbyjobid -j ${jobId} -sp '${opts.storagePolicyName}' -spc '${opts.storagePolicyCopyName}' -ft Q_DATA_LOG"

		def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/QCommand", authConfig.token, [format:'text/xml', body: body], 'POST')

		if(results.success) {
			def contentStr = results.content?.toString().toLowerCase()
			if(contentStr?.contains("pruned successfully") || contentStr.contains("no jobs to prune")) {
				rtn.success = true
			} else {
				rtn.msg = "Error deleting backup."
			}
		}

		return rtn
	}

	static deleteVMClient(authConfig, vmGuid) {
		def rtn = [success:false]
		authConfig.token = authConfig.token ?: getToken(authConfig.apiUrl, authConfig.username, authConfig.password)?.token
		def vmQuery = [guid: vmGuid]
		def vmResults = callApi(authConfig.apiUrl, "${authConfig.basePath}/VM", authConfig.token, [format:'xml', query: vmQuery], 'GET')
		if(vmResults.success) {
			def vmResponse = new XmlSlurper().parseText(vmResults?.content)
			def clientId = vmResponse.vmStatusInfoList.client['@clientId']
			if(clientId && clientId != "") {
				def results = callApi(authConfig.apiUrl, "${authConfig.basePath}/Client/${clientId}", authConfig.token, [format:'xml'], 'DELETE')
				if(results.success) {
					rtn.success = true
				}
			} else {
				rtn.success = true
			}
		}
		return rtn
	}

	static getToken(url, username, password) {
		def rtn = [success:false]
		password = password.getBytes().encodeBase64().toString()
		def requestWriter = new StringWriter()
		def xml = new groovy.xml.MarkupBuilder(requestWriter)
		xml.DM2ContentIndexing_CheckCredentialReq(username:"${username}", password:"${password}")
		def body = requestWriter.toString()
		def results = callApi(url, '/SearchSvc/CVWebService.svc/Login', null, [format:'xml', body:body], 'POST')
		rtn.success = results?.success
		rtn.errorCode = results?.errorCode
		if(rtn.success == true && (rtn.errorCode == null || rtn.errorCode < 400)) {
			//
			def response = results?.content ? new XmlSlurper().parseText(results.content) : null
			def responseErrors = response ? response.childNodes().findAll() { it.name == "errList" }.collect { [error: it.attributes.errLogMessage, errorCode: it.attributes.errorCode] } : []
			if(responseErrors == null || responseErrors?.size() == 0) {
				def token = response['@token'].toString()
				rtn.token = token
			} else {
				rtn.success = false
				if(responseErrors?.getAt(0)?.errorCode == '1116') {
					rtn.errorCode = 401
				} else {
					rtn.errorCode = responseErrors?.getAt(0)?.errorCode?.toInteger()
				}
				rtn.msg = results.error ?: responseErrors?.getAt(0)?.error ?: 'unknown error connecting to commvault'
				log.info("Error getting commvault token, error code ${rtn.errorCode}: ${rtn.msg}")
			}
		} else {
			rtn.success = false
			rtn.msg = results.error ?: "${rtn.errorCode} - unknown error connecting to commvault"
		}
		return rtn
	}

	static logout(url, token){
		def rtn = [success:false]
		def results = callApi(url, "/SearchSvc/CVWebService.svc/Logout", token, [format:'xml'], 'POST')
		log.debug("got: ${results}")
		rtn.success = results?.success
		return rtn
	}

	static buildHeaders(token, opts) {
		def rtn = [
			'Cookie2': token,
			'Accept': opts.format == 'json' ? 'application/json' : 'application/xml'
		]

		return rtn
	}

	static callApi(url, path, token, opts = [:], method) {
		def rtn = ServiceResponse.prepare()
		def httpApiClient = new HttpApiClient()
		def requestOpts = new HttpApiClient.RequestOptions()
		try {
			log.debug("calling: ${url}${path} - token: ${token}")
			String connectTimeout = System.getProperty('com.gomorpheus.commvault.connect.timeout', '5000')
			String readTimeout = System.getProperty('com.gomorpheus.commvault.read.timeout', '30000')
			opts.connectTimeout = opts.connectTimeout ?: connectTimeout?.toInteger()
			opts.readTimeout = opts.readTimeout ?: readTimeout?.toInteger()
			opts.headers = buildHeaders(token, opts)

			requestOpts.body = opts.body
			requestOpts.contentType = "application/xml"  // Since format is xml
			requestOpts.headers = opts.headers.findAll {k, v -> v != null}
			requestOpts.connectionTimeout = opts.connectTimeout
			requestOpts.readTimeout = opts.readTimeout
			requestOpts.queryParams = opts.query

			if(opts.format == 'json') {
				rtn = httpApiClient.callJsonApi(url, path, null, null, requestOpts, method)
			} else if(opts.format == 'text/xml') {
				opts.headers['Content-Type'] = 'text/plain'
				rtn = httpApiClient.callApi(url, path, null, null, requestOpts, method)
				rtn.data = [:]
				if(rtn.content?.length() > 0) {
					try {
						rtn.data =  new XmlSlurper(false,true).parseText(rtn.content)
					} catch(e) {
						log.debug("Error parsing API response XML: ${e}", e)
					}
				}
			} else {
				rtn = httpApiClient.callXmlApi(url, path, null, null, requestOpts, method)
			}
			if(rtn.success == false && rtn.errorCode && !rtn.errors) {
				rtn.msg = getApiResultsErrorMessage(rtn.data)
			} else if(rtn.success == false && !rtn.errorCode) {
				// attempt to set the response error code and message
				def responseError = getApiResultsError(rtn.data)
				if(responseError.success) {
					rtn.errorCode = responseError.errorCode
					rtn.error = responseError.errorMessage
				}
			}
		} catch(e) {
			log.error("callApi error: ${e}:", e)
			rtn.error = e.message
		}
		return rtn
	}

	static getApiResultsError(apiResults) {
		def rtn = [success: false]
		try {
			def responseData
			if(apiResults.data) {
				responseData = apiResults.data
			} else {
				responseData = apiResults
			}

			rtn.errors = getApiResultsErrorMessage(responseData)
			rtn.errorCode = getApiResultsErrorCode(responseData)
			rtn.success = true
		} catch(Exception ex) {
			log.error("getErrorResponse error: {}", ex, ex)
		}

		return rtn
	}

	static getApiResultsErrorMessage(responseData) {
		return responseData["@errorMessage"]
	}

	static getApiResultsErrorCode(responseData) {
		return responseData["@errorCode"]
	}

	static captureActiveSubclientBackup(authConfig, subclientId, clientId, backupsetId) {
		def activeJobsResults = CommvaultApiUtility.getBackupJobs(authConfig, subclientId, [query: [clientId: clientId, backupsetId: backupsetId, jobFilter: "Backup", jobCategory: "Active"]])
		if(activeJobsResults.success && activeJobsResults.results.size() > 0) {
			def activeBackupJob = activeJobsResults.results.find { it.clientId == clientId && it.subclientId == subclientId && it.backupsetId == backupsetId }
			return ServiceResponse.success([backupJobId: activeBackupJob.backupJobId])
		}
	}

}