package com.morpheusdata.commvault

import com.morpheusdata.commvault.sync.StoragePoliciesSync
import com.morpheusdata.commvault.sync.SubclientsSync
import com.morpheusdata.commvault.sync.ClientSync
import com.morpheusdata.commvault.utils.CommvaultBackupUtility
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j

@Slf4j
class CommvaultBackupProvider extends AbstractBackupProvider {

	BackupJobProvider backupJobProvider;
	private CommvaultPlugin plugin

	CommvaultBackupProvider(CommvaultPlugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
		this.plugin = plugin

		CommvaultBackupTypeProvider backupTypeProvider = new CommvaultBackupTypeProvider(plugin, morpheus)
		plugin.registerProvider(backupTypeProvider)
		addScopedProvider(backupTypeProvider, "vmware", null)
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'commvault'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Commvault'
	}
	
	/**
	 * Returns the integration logo for display when a user needs to view or add this integration
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:"icon.svg", darkPath: "icon-dark.svg")
	}

	/**
	 * Sets the enabled state of the provider for consumer use.
	 */
	@Override
	public Boolean getEnabled() { return true; }

	/**
	 * The backup provider is creatable by the end user. This could be false for providers that may be
	 * forced by specific CloudProvider plugins, for example.
	 */
	@Override
	public Boolean getCreatable() { return true; }
	
	/**
	 * The backup provider supports restoring to a new workload.
	 */
	@Override
	public Boolean getRestoreNewEnabled() { return true; }

	/**
	 * The backup provider supports backups. For example, a backup provider may be intended for disaster recovery failover
	 * only and may not directly support backups.
	 */
	@Override
	public Boolean getHasBackups() { return true; }

	/**
	 * The backup provider supports creating new jobs.
	 */
	@Override
	public Boolean getHasCreateJob() { return true; }

	/**
	 * The backup provider supports cloning a job from an existing job.
	 */
	@Override
	public Boolean getHasCloneJob() { return true; }

	/**
	 * The backup provider can add a workload backup to an existing job.
	 */
	@Override
	public Boolean getHasAddToJob() { return true; }

	/**
	 * The backup provider supports backups outside an encapsulating job.
	 */
	@Override
	public Boolean getHasOptionalJob() { return true; }

	/**
	 * The backup provider supports scheduled backups. This is primarily used for display of hte schedules and providing
	 * options during the backup configuration steps.
	 */
	@Override
	public Boolean getHasSchedule() { return true; }

	/**
	 * The backup provider supports running multiple workload backups within an encapsulating job.
	 */
	@Override
	public Boolean getHasJobs() { return true; }

	/**
	 * The backup provider supports retention counts for maintaining the desired number of backups.
	 */
	@Override
	public Boolean getHasRetentionCount() { return true; }

	/**
	 * Get the list of option types for the backup provider. The option types are used for creating and updating an
	 * instance of the backup provider.
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList();
		optionTypes << new OptionType(
				code:"backupProviderType.commvault.hostUrl", inputType:OptionType.InputType.TEXT, name:'host', category:"backupProviderType.commvault",
				fieldName:'host', fieldCode: 'gomorpheus.optiontype.ApiUrl', fieldLabel:'Host', fieldContext:'domain', fieldGroup:'default',
				required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:10, fieldClass:null
		)
		optionTypes << new OptionType(
				code:"backupProviderType.commvault.port", inputType:OptionType.InputType.NUMBER, name:'port', category:"backupProviderType.commvault",
				fieldName:'port', fieldCode: 'gomorpheus.optiontype.Port', fieldLabel:'Port', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:15, fieldClass:null
		)
		optionTypes << new OptionType(
				code:"backupProviderType.commvault.credential", inputType:OptionType.InputType.CREDENTIAL, name:'credentials', category:"backupProviderType.commvault",
				fieldName:'type', fieldCode:'gomorpheus.label.credentials', fieldLabel:'Credentials', fieldContext:'credential', optionSource:'credentials',
				required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:'local', custom:false,
				displayOrder:25, fieldClass:null, wrapperClass:null, config: JsonOutput.toJson([credentialTypes:['username-password']]).toString()
		)
		optionTypes << new OptionType(
				code:"backupProviderType.commvault.username", inputType:OptionType.InputType.TEXT, name:'username', category:"backupProviderType.commvault",
				fieldName:'username', fieldCode: 'gomorpheus.optiontype.Username', fieldLabel:'Username', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:30, fieldClass:null, localCredential:true
		)
		optionTypes << new OptionType(
				code:"backupProviderType.commvault.password", inputType:OptionType.InputType.PASSWORD, name:'password', category:"backupProviderType.commvault",
				fieldName:'password', fieldCode: 'gomorpheus.optiontype.Password', fieldLabel:'Password', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:35, fieldClass:null, localCredential:true
		)
		return optionTypes
	}

	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating and updating
	 * replication groups.
	 */
	@Override
	Collection<OptionType> getReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}
	
	/**
	 * Get the list of replication option types for the backup provider. The option types are used for creating and updating
	 * replications.
	 */
	@Override
	Collection<OptionType> getReplicationOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the list of backup job option types for the backup provider. The option types are used for creating and updating
	 * backup jobs.
	 */
	@Override
	Collection<OptionType> getBackupJobOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of backup option types for the backup provider. The option types are used for creating and updating
	 * backups.
	 */
	@Override
	Collection<OptionType> getBackupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}
	
	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating
	 * replications on an instance during provisioning.
	 */
	@Override
	Collection<OptionType> getInstanceReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the {@link BackupJobProvider} responsible for all backup job operations in this backup provider
	 * The {@link DefaultBackupJobProvider} can be used if the provider would like morpheus to handle all job operations.
	 * @return the {@link BackupJobProvider} for this backup provider
	 */
	@Override
	BackupJobProvider getBackupJobProvider() {
		// The default backup job provider allows morpheus to handle the
		// scheduling and execution of the jobs. Replace the default job provider
		// if jobs are to be managed on the external backup system.
		if(!this.backupJobProvider) {
			this.backupJobProvider = new DefaultBackupJobProvider(getPlugin(), morpheus);
		}
		return this.backupJobProvider
	}

	/**
	 * Apply provider specific configurations to a {@link com.morpheusdata.model.BackupProvider}. The standard configurations are handled by the core system.
	 * @param backupProviderModel backup provider to configure
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackupProvider(BackupProviderModel backupProviderModel, Map config, Map opts) {
		return ServiceResponse.success(backupProviderModel)
	}

	/**
	 * Validate the configuration of the {@link com.morpheusdata.model.BackupProvider}. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupProviderModel backup provider to validate
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * validation and will halt the backup provider creation process.
	 */
	@Override
	ServiceResponse validateBackupProvider(BackupProvider backupProvider, Map opts) {
		log.debug "validateBackupProvider: ${backupProvider}, opts: ${opts}"
		def rtn = [success:false, errors:[:]]
		try {
			def apiOpts = [:]
			//validate input fields
			rtn.data = backupProvider
			//credentials
			def credential = morpheus.async.accountCredential.loadCredentialConfig(opts.credential, [username: backupProvider.username, password: backupProvider.password]).blockingGet()
			if(!credential.data?.username) {
				rtn.errors.username = 'Enter a username'
				rtn.msg = 'Missing required parameter'
			}
			if(!credential.data?.password) {
				rtn.errors.password = 'Enter a password'
				rtn.msg = 'Missing required parameter'
			}
			if(!rtn.errors) {
				backupProvider.credentialData = credential.data
				backupProvider.credentialLoaded = true
				def testResults = testConnection(backupProvider, apiOpts)
				log.debug("api test results: {}", testResults)
				if (testResults.success == true)
					rtn.success = true
				else if (testResults.invalidLogin == true)
					rtn.msg = testResults.msg ?: 'unauthorized - invalid credentials'
				else if (testResults.found == false)
					rtn.msg = testResults.msg ?: 'commvault not found - invalid host'
				else
					rtn.msg = testResults.msg ?: 'unable to connect to commvault'
			}
		} catch(e) {
			log.error("error validating commvault configuration: ${e}", e)
			rtn.msg = 'unknown error connecting to commvault'
			rtn.success = false
		}
		return ServiceResponse.create(rtn)
	}

	def testConnection(BackupProvider backupProvider, Map opts) {
		def rtn = [success:false, invalidLogin:false, found:true]
		opts.authConfig = opts.authConfig ?: plugin.getAuthConfig(backupProvider)
		def tokenResults = loginSession(opts.authConfig.apiUrl, opts.authConfig.username, opts.authConfig.password)
		if(tokenResults.success == true) {
			rtn.success = true
			def token = tokenResults.token
			def sessionId = tokenResults.sessionId
			logoutSession(opts.authConfig.apiUrl, token)
		} else {
			if(tokenResults?.errorCode == '404' || tokenResults?.errorCode == 404)
				rtn.found = false
			if(tokenResults?.errorCode == '401' || tokenResults?.errorCode == 401)
				rtn.invalidLogin = true
			rtn.msg = tokenResults.msg
			rtn.errorCode = tokenResults.errorCode
		}
		return rtn
	}

	def loginSession(String apiUrl, String username, String password) {
		def rtn = [success: false]
		def response = CommvaultBackupUtility.getToken(apiUrl, username, password)
		if(response.success) {
			rtn.success = true
			rtn.token = response.token
		} else {
			rtn.success = false
			rtn.msg = response.msg
			rtn.errorCode = response.errorCode
		}
		return rtn
	}

	def logoutSession(String apiUrl, String token) {
		if(token) {
			CommvaultBackupUtility.logout(apiUrl, token)
		}
	}

	/**
	 * Delete the backup provider. Typically used to clean up any provider specific data that will not be cleaned
	 * up by the default remove in the core system.
	 * @param backupProviderModel the backup provider being removed
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * delete and will halt the process.
	 */
	@Override
	ServiceResponse deleteBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * The main refresh method called periodically by Morpheus to sync any necessary objects from the integration.
	 * This can call sub services for better organization. It is recommended that {@link com.morpheusdata.core.util.SyncTask} is used.
	 * @param backupProvider the current instance of the backupProvider being refreshed
	 * @return the success state of the refresh
	 */
	@Override
	ServiceResponse refresh(BackupProvider backupProvider) {
		log.debug("refresh backup provider: {}", backupProvider)
		ServiceResponse response = ServiceResponse.prepare()
		try {
			def authConfig = plugin.getAuthConfig(backupProvider)
			def apiOpts = [authConfig: authConfig]
			def apiUrl = authConfig.apiUrl
			def apiUrlObj = new URL(apiUrl)
			def apiHost = apiUrlObj.getHost()
			def apiPort = apiUrlObj.getPort() > 0 ? apiUrlObj.getPort() : (apiUrlObj?.getProtocol()?.toLowerCase() == 'https' ? 443 : 80)
			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)
			log.debug("commvault host online: {}", hostOnline)
			if (hostOnline) {
				def testResults = testConnection(backupProvider, apiOpts)
				log.debug("testResults: ${testResults}")
				if (testResults.success == true) {
					morpheus.async.backupProvider.updateStatus(backupProvider, 'ok', null).subscribe().dispose()

					def now = new Date().time
					new ClientSync(morpheus, backupProvider, authConfig).execute()
					log.debug("ClientSync in ${new Date().time - now}ms")
          
					now = new Date().time
					new SubclientsSync(backupProvider, plugin).execute()
					log.info("${backupProvider.name}: SubclientsSync in ${new Date().time - now}ms")

					now = new Date().time
					new StoragePoliciesSync(morpheus, backupProvider, authConfig).execute()
					log.info("StoragePoliciesSync in ${new Date().time - now}ms")

					response.success = true
				} else {
					if (testResults.invalidLogin == true) {
						morpheus.async.backupProvider.updateStatus(backupProvider, 'error', 'invalid credentials').subscribe().dispose()
					} else if (testResults.found == false) {
						morpheus.async.backupProvider.updateStatus(backupProvider, 'error', 'commvault not found - invalid host').subscribe().dispose()
					} else {
						morpheus.async.backupProvider.updateStatus(backupProvider, 'error', 'unable to connect to commvault').subscribe().dispose()
					}
				}
			} else {
				morpheus.async.backupProvider.updateStatus(backupProvider, 'offline', 'commvault not reachable').subscribe().dispose()
			}
		} catch (e) {
			log.error("refresh BackupProvider error: ${e}", e)
		}
		return response
	}
}
