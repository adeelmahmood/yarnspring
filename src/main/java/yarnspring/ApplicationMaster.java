package yarnspring;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMaster {

	private static final Logger log = LoggerFactory.getLogger(ApplicationMaster.class);

	private Configuration conf;
	private FileSystem fs;

	// handle to communicate with RM
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;

	private UserGroupInformation ugi;

	// handle to communicate with NM
	private NMClientAsync nmClient;
	// callback listener for NM events
	private NMCallbackHandler nmCallbackHandler;

	private ApplicationAttemptId appAttemptId;

	// client tracking info
	private String amHostName = "";
	private int amRpcPort = -1;
	private String amTrackingUrl = "";

	private String containerJarPath = "";
	private int numContainers = 1;
	private int numContainersToRequest = 1;
	private int containerMem = 10;
	private int containerVCores = 1;

	private AtomicInteger numRequestedContainers = new AtomicInteger();
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();

	private volatile boolean done;
	private ByteBuffer allTokens;
	private boolean debug;
	private int priority;

	private List<Thread> launchThreads = new ArrayList<Thread>();

	private Options opts;

	public static void main(String[] args) {
		boolean result = false;
		try {
			ApplicationMaster am = new ApplicationMaster();
			try {
				// initialize AM
				boolean run = am.init(args);
				if (!run) {
					System.exit(0);
				}
				result = am.run();
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				am.printHelp();
				System.exit(-1);
			}
		} catch (Throwable t) {
			log.error("error in application master", t);
			ExitUtil.terminate(1, t);
		}
		if (result) {
			log.info("application master completed successfully");
			System.exit(0);
		} else {
			log.info("application master ended with failure");
			System.exit(2);
		}
	}

	public ApplicationMaster() throws IOException {
		conf = new YarnConfiguration();
		fs = FileSystem.get(conf);

		// specify command line options
		opts = new Options();
		opts.addOption("num_containers", true, "Number of containers to launch");
		opts.addOption("container_mem", true, "Container memory. Default is 10");
		opts.addOption("container_vcores", true, "Number of virtual cores for containers to launch");
		opts.addOption("container_priority", true, "Priority for containers to launch");
		opts.addOption("app_attempt_id", true, "Application attempt id, needed for testing");
		opts.addOption("debug", false, "Enable debugging output");
		opts.addOption("help", false, "Display help");
	}

	private void printHelp() {
		new HelpFormatter().printHelp("Application Master", opts);
	}

	public boolean init(String[] args) throws ParseException {
		// parse command line params
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (args.length == 0) {
			throw new IllegalArgumentException("No arguments specified");
		}

		if (cliParser.hasOption("help")) {
			printHelp();
			return false;
		}

		debug = cliParser.hasOption("debug");

		Map<String, String> env = System.getenv();

		// gather application attempt id (attempt id because of possible
		// failure for AM)
		if (!env.containsKey(Environment.CONTAINER_ID.name())) {
			// expect it to be passed in
			if (cliParser.hasOption("app_attempt_id")) {
				String appAttemptIdStr = cliParser.getOptionValue("app_attempt_id");
				appAttemptId = ConverterUtils.toApplicationAttemptId(appAttemptIdStr);
			} else {
				throw new IllegalArgumentException("app_attempt_id not set in environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(env.get(Environment.CONTAINER_ID.name()));
			appAttemptId = containerId.getApplicationAttemptId();
		}

		YarnUtils.checkApplicationEnvRequiredParams(env);

		log.info("Application master for app" + ", appId=" + appAttemptId.getApplicationId().getId()
				+ ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp() + ", attemptId="
				+ appAttemptId.getAttemptId());

		// container properties
		containerJarPath = env.get("CONTAINER_JAR");
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		containerMem = Integer.parseInt(cliParser.getOptionValue("container_mem", "10"));
		containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		priority = Integer.parseInt(cliParser.getOptionValue("container_priority", "0"));
		if (numContainers < 0 || containerMem < 0 || containerVCores < 0) {
			throw new IllegalArgumentException(
					"Invalid values specified for num_container, container_mem or container_vcores as ["
							+ numContainers + "," + containerMem + "," + containerVCores + "]");
		}

		log.info("application master initialized");
		return true;
	}

	public boolean run() throws IOException, YarnException {
		log.info("running aplication master");
		if (debug) {
			YarnUtils.printEnvAndClasspath();
		}

		// gather submitted credentials
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		log.info("Executing with tokens:");
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			log.info(token.toString());
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

		// Create appSubmitterUgi and add original tokens to it
		String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
		ugi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
		ugi.addCredentials(credentials);

		// initialize AMRMClient
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
		amRMClient.init(conf);
		amRMClient.start();
		log.info("application master to resource manager connection established");

		// initialize NM callback handler
		nmCallbackHandler = new NMCallbackHandler(this);
		nmClient = new NMClientAsyncImpl(nmCallbackHandler);
		nmClient.init(conf);
		nmClient.start();
		log.info("application master to node manager connection established");

		// register with RM and start heartbeat
		amHostName = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(amHostName, amRpcPort,
				amTrackingUrl);

		// get container capabilities and adjust our requirements
		int maxMem = response.getMaximumResourceCapability().getMemory();
		log.info("maximum memory allowed => " + maxMem);
		if (containerMem > maxMem) {
			log.info("request container memory " + containerMem + " is more than max memory " + maxMem + ", adjusting");
			containerMem = maxMem;
		}

		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		log.info("maximum virtual cores allowed => " + maxVCores);
		if (containerVCores > maxVCores) {
			log.info("requested virtual cores " + containerVCores + " is more than max virtual cores " + maxVCores
					+ ", adjusting");
			containerVCores = maxVCores;
		}

		// get previously running containers
		List<Container> previouslyRunningContainers = response.getContainersFromPreviousAttempts();
		log.info("found " + previouslyRunningContainers.size() + " previously running containers");
		numAllocatedContainers.addAndGet(previouslyRunningContainers.size());

		// request containers
		numContainersToRequest = numContainers - previouslyRunningContainers.size();
		requestForContainers();

		return finish();
	}

	@SuppressWarnings("unchecked")
	protected void requestForContainers() {
		int totalContainersToRequest = numContainersToRequest - numRequestedContainers.get();
		if (totalContainersToRequest > 0) {
			log.info("requesting " + totalContainersToRequest + " new containers");
			for (int i = 0; i < totalContainersToRequest; ++i) {
				ContainerRequest containerRequest = requestForContainer();
				amRMClient.addContainerRequest(containerRequest);
			}
		}
		numRequestedContainers.addAndGet(totalContainersToRequest);
		// if(numRequestedContainers.get() == numContainers){
		// log.info("all containers requested, marking as done");
		// done = true;
		// }
	}

	private ContainerRequest requestForContainer() {
		// setup priority
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(priority);

		// setup resource type requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMem);
		capability.setVirtualCores(containerVCores);

		// create container request
		ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		return request;
	}

	private boolean finish() {
		// wait for completion
		while (!done && numCompletedContainers.get() != numContainers) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}

		// complete all threads
		for (Thread t : launchThreads) {
			try {
				t.join(10000);
			} catch (InterruptedException e) {
				log.error("error in completing thread", e);
			}
		}

		log.info("signalling NM to stop all containers");
		// signal NM to stop all containers
		nmClient.stop();

		// figure out application final status
		FinalApplicationStatus finalStatus;
		String retMessage = null;
		boolean success = true;
		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numContainers) {
			finalStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			finalStatus = FinalApplicationStatus.FAILED;
			retMessage = "Diagnostics." + ", total=" + numContainers + ", completed=" + numCompletedContainers.get()
					+ ", allocated=" + numAllocatedContainers.get() + ", failed=" + numFailedContainers.get();
			success = false;
		}

		try {
			// signal RM for completion
			log.info("signalling RM to unregister and stop");
			amRMClient.unregisterApplicationMaster(finalStatus, retMessage, amTrackingUrl);
		} catch (YarnException | IOException e) {
			log.error("error in unregistering AM", e);
		}
		amRMClient.stop();
		return success;
	}

	protected void registerContainerContext(Container container, ContainerLaunchContext context) {
		log.info("registering container context for container " + container.getId());
		nmCallbackHandler.addContainer(container.getId(), container);
		nmClient.startContainerAsync(container, context);
	}

	protected void getContainerStatus(ContainerId containerId, Container container) {
		nmClient.getContainerStatusAsync(containerId, container.getNodeId());
	}

	protected void addLaunchThread(Thread t) {
		launchThreads.add(t);
	}

	protected void isDone(boolean done) {
		this.done = done;
	}

	protected float getAMProgress() {
		float progress = (float) numCompletedContainers.get() / numContainers;
		return progress;
	}

	protected void containerFailed() {
		numCompletedContainers.incrementAndGet();
		numFailedContainers.incrementAndGet();
		log.info("application master received notification for failed container");
	}

	protected void containerKilled() {
		numAllocatedContainers.decrementAndGet();
		numRequestedContainers.decrementAndGet();
		log.info("application master received notification for killed container");
	}

	protected void containerCompleted() {
		numCompletedContainers.incrementAndGet();
		log.info("application master received notification for completed container");
	}

	protected ByteBuffer getTokens() {
		return allTokens.duplicate();
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			log.info(statuses.size() + " containers responded with completed status");
			for (ContainerStatus status : statuses) {
				log.info("container status for containerId=" + status.getContainerId() + ", state=" + status.getState()
						+ ", exitStatus=" + status.getExitStatus() + ", diagnostics=" + status.getDiagnostics());
				// make sure the return state in completed
				if (status.getState() != ContainerState.COMPLETE) {
					throw new RuntimeException("unexpected container in completed status event");
				}

				// check exit status and adjust progress accordingly
				int exitStatus = status.getExitStatus();
				if (exitStatus != 0) {
					// failed on its own
					if (ContainerExitStatus.ABORTED != exitStatus) {
						containerFailed();
					}
					else {
						// killed by framework
						containerKilled();
					}
				} else {
					// completed successfully
					containerCompleted();
				}

				// trigger request for new containers if some were killed
				requestForContainers();
			}
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			log.info(containers.size() + " containers allocated");
			for (Container container : containers) {
				log.info("Allocated container, containerId=" + container.getId() + ", containerNode="
						+ container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
						+ ", containerNodeURI=" + container.getNodeHttpAddress() + ", containerResourceMemory"
						+ container.getResource().getMemory() + ", containerResourceVirtualCores"
						+ container.getResource().getVirtualCores());

				// launch container
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(container);
				Thread t = new Thread(runnableLaunchContainer);
				// start container thread in a separate thread to keep the main
				// thread free
				addLaunchThread(t);
				t.start();
			}
		}

		@Override
		public void onShutdownRequest() {
			isDone(true);
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		@Override
		public float getProgress() {
			return getAMProgress();
		}

		@Override
		public void onError(Throwable e) {
			isDone(true);
		}
	}

	static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private final ApplicationMaster am;

		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

		public NMCallbackHandler(ApplicationMaster am) {
			this.am = am;
		}

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			log.info("container " + containerId + " successfully started");
			// get container instance
			Container container = containers.get(containerId);
			if (container != null) {
				// register for status events
				am.getContainerStatus(containerId, container);
			}
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			log.info("container = " + containerId + ", status = " + containerStatus);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			containers.remove(containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			log.warn("failed to start container " + containerId);
			containers.remove(containerId);
			am.containerFailed();
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			log.warn("failed to query the status of container " + containerId + ", error message = "
					+ t.getLocalizedMessage());
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			log.warn("failed to stop container " + containerId);
			containers.remove(containerId);
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}
	}

	private class LaunchContainerRunnable implements Runnable {

		private final Container container;

		public LaunchContainerRunnable(Container container) {
			this.container = container;
		}

		@Override
		public void run() {
			log.info("setting up launch container for " + container.getId());
			ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);

			// setup local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

			try {
				//add containers jar to local resources
				ResourceUtils.addLocalResource(fs, containerJarPath, localResources);
			} catch (IOException e) {
				log.error("error in adding containers jar as local resource", e);
			}
			
			// set local resources on launch context
			context.setLocalResources(localResources);

			// create environment
			Map<String, String> env = new HashMap<String, String>();

			// set classpath
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(
					ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			env.put("CLASSPATH", classPathEnv.toString());
			if (debug) {
				log.info("Classpath set as => " + classPathEnv.toString());
			}

			// set the environment
			context.setEnvironment(env);

			// set the launch command
			Vector<CharSequence> vargs = new Vector<CharSequence>();
			// specify command
			// vargs.add("/bin/date");
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			vargs.add("-Xmx" + containerMem + "m");
			// // set classname
			vargs.add("-jar");
			vargs.add("Containers.jar");
			// Add log redirect params
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

			// set command on container
			context.setCommands(YarnUtils.createCommand(vargs));

			// set security tokens for container
			context.setTokens(getTokens());

			// finalize registering of this container
			registerContainerContext(container, context);
		}
	}
}