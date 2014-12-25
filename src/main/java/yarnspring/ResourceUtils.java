package yarnspring;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ResourceUtils {

	private static final Log log = LogFactory.getLog(ResourceUtils.class);

	public static void addLocalResource(FileSystem fs, String srcPath, String destPath, String appName, String appId,
			Map<String, LocalResource> localResources) throws IOException {
		String suffix = appName + "/" + appId + "/" + destPath;
		Path dest = new Path(fs.getHomeDirectory(), suffix);

		// copy local file into destination file system
		fs.copyFromLocalFile(new Path(srcPath), dest);

		// get file status for resource from dest fs
		FileStatus status = fs.getFileStatus(dest);
		// create new local resource
		LocalResource localResource = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dest.toUri()),
				LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, status.getLen(),
				status.getModificationTime());
		// add to local resources list
		localResources.put(destPath, localResource);
		log.info("successfully added local resource as " + localResource);
	}

	public static void addLocalResource(FileSystem fs, String path, Map<String, LocalResource> localResources)
			throws IOException {
		Path p = new Path(path);
		FileStatus status = fs.getFileStatus(p);
		// create new local resource
		LocalResource localResource = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(p.toUri()),
				LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, status.getLen(),
				status.getModificationTime());
		// add to local resources list
		localResources.put(FilenameUtils.getName(path), localResource);
		log.info("successfully added local resource as " + localResource);
	}
}
