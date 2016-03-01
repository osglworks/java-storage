package org.osgl.storage.impl;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import org.osgl.exception.ConfigurationException;
import org.osgl.exception.UnexpectedIOException;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.S;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class BlobService extends StorageServiceBase implements IStorageService {

    private static Logger log = LoggerFactory.getLogger(BlobService.class);

    public static final String CONF_PROTOCOL = "storage.blob.protocol";
    public static final String CONF_ACCOUNT_NAME = "storage.blob.account.name";
    public static final String CONF_ACCOUNT_KEY = "storage.blob.account.key";
    public static final String CONF_BUCKET = "storage.blob.bucket";

    private final static String CONNECTION_PATTERN = "DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};";
    private final static String URL_PATTERN = "http://{0}.blob.core.windows.net/{1}/{2}";

    private CloudBlobClient blobClient;
    private CloudBlobContainer blobContainer;
    private String accountName;

    private boolean noGet = false;
    private String staticWebEndPoint = null;

    public BlobService(Map<String, String> conf) {
        String protocol = getOrDefault(conf, CONF_PROTOCOL, "");
        this.accountName = getOrDefault(conf, CONF_ACCOUNT_NAME, "");
        String accountKey = getOrDefault(conf, CONF_ACCOUNT_KEY, "");

        connect(protocol, accountName, accountKey, conf.get(CONF_BUCKET));
    }

    //In Java 6 there is not map.getOrDefault()
    private String getOrDefault(Map<String, String> map, String key, String defaultValue) {
        if (map.get(key) == null) return defaultValue;
        return map.get(key);
    }

    /**
     * Create a connection to Azure Blob file storage system
     */
    private void connect(String protocol, String accountName, String accountKey, String bucketName) {
        if (bucketName == null || bucketName.trim().isEmpty())
            throw new ConfigurationException("Defined Azure Blog bucket is invalid.");
        //container name MUST be lowercase
        bucketName = bucketName.toLowerCase();

        String connectionString = MessageFormat.format(CONNECTION_PATTERN, protocol, accountName, accountKey);
        try {
            CloudStorageAccount blobAccount = CloudStorageAccount.parse(connectionString);

            this.blobClient = blobAccount.createCloudBlobClient();
            this.blobContainer = blobClient.getContainerReference(bucketName);

            boolean isBucketNotExist = blobContainer.createIfNotExists();
            if (isBucketNotExist)
                log.info("New Azure Blob container created: " + bucketName);

            //Set access to public for blob resource
            BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            blobContainer.uploadPermissions(containerPermissions);
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
            throw new ConfigurationException(exception.getMessage());
        }
    }

    /**
     * Get a resource from Azure Blob based on the resource key
     */
    @Override
    public ISObject get(String resourceKey) {
        if (noGet) {
            return SObject.getDumpObject(resourceKey);
        }
        ISObject sobj = new BlobObject(resourceKey, this);
        sobj.setAttribute(ISObject.ATTR_SS_ID, id());
        sobj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
        if (null != staticWebEndPoint) {
            sobj.setAttribute(ISObject.ATTR_URL, getUrl(resourceKey));
        }
        return sobj;
    }

    @Override
    public ISObject getFull(String key) {
        ISObject sobj = new BlobObject(key, this);
        sobj.setAttribute(ISObject.ATTR_SS_ID, id());
        if (null != staticWebEndPoint) {
            sobj.setAttribute(ISObject.ATTR_URL, getUrl(key));
        }
        return sobj;
    }

    @Override
    public ISObject loadContent(ISObject sobj0) {
        String key = sobj0.getKey();
        ISObject sobj = new BlobObject(key, this);
        sobj.setAttribute(ISObject.ATTR_SS_ID, id());
        sobj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
        if (null != staticWebEndPoint) {
            sobj.setAttribute(ISObject.ATTR_URL, getUrl(key));
        }
        return sobj;
    }

    @Override
    public ISObject put(String resourceKey, ISObject blobObject) throws UnexpectedIOException {
        if (blobObject instanceof BlobObject && S.eq(resourceKey, blobObject.getKey()) && S.eq(id(), blobObject.getAttribute(ISObject.ATTR_SS_ID)) && S.eq(contextPath(), blobObject.getAttribute(ISObject.ATTR_SS_CTX))) {
            return blobObject;
        }

        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(resourceKey);
            blob.upload(blobObject.asInputStream(), blobObject.getLength());
            blob.getProperties().setContentType(blobObject.getAttribute("content-type"));
            blob.uploadProperties();

            ISObject sobj = new BlobObject(resourceKey, this);
            sobj.setAttribute(ISObject.ATTR_SS_ID, id());
            sobj.setAttribute(ISObject.ATTR_SS_CTX, contextPath());
            if (null != staticWebEndPoint) {
                sobj.setAttribute(ISObject.ATTR_URL, getUrl(resourceKey));
            }
            return sobj;
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
            throw new UnexpectedIOException(exception.getMessage());
        }
    }

    @Override
    public void remove(String resourceKey) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(resourceKey);
            blob.deleteIfExists();
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
            throw new UnexpectedIOException(exception.getMessage());
        }
    }

    @Override
    public String getUrl(String resourceKey) {
        return MessageFormat.format(URL_PATTERN, accountName, blobContainer.getName(), resourceKey);
    }

    @Override
    protected IStorageService createSubFolder(String contextPath) {
        BlobService subFolder = new BlobService(conf);
        subFolder.keygen = this.keygen;
        subFolder.contextPath = keyWithContextPath(contextPath);
        return subFolder;
    }

    public Map<String,String> getMeta(String resourceKey) {
        HashMap<String, String> metadata = new HashMap<String, String>();
        try {
            metadata.putAll(blobContainer.getBlockBlobReference(resourceKey).getMetadata());
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
        }
        metadata.put(ISObject.ATTR_SS_ID, id());
        if (null != staticWebEndPoint) {
            metadata.put(ISObject.ATTR_URL, getUrl(resourceKey));
        }
        return metadata;
    }

    public InputStream getInputStream(String resourceKey) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(resourceKey);
            return blob.openInputStream();
        } catch (Exception exception) {
            log.error(exception.getMessage(), exception);
        }
        return null;
    }
}
