package org.osgl.storage.impl;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import org.osgl.exception.ConfigurationException;
import org.osgl.storage.ISObject;
import org.osgl.storage.IStorageService;
import org.osgl.util.C;
import org.osgl.util.E;
import org.osgl.util.S;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Map;

public class AzureService extends StorageServiceBase<AzureObject> implements IStorageService {

    private static Logger log = LoggerFactory.getLogger(AzureService.class);

    public static final String CONF_PROTOCOL = "storage.azure.protocol";
    public static final String CONF_ACCOUNT_NAME = "storage.azure.account.name";
    public static final String CONF_ACCOUNT_KEY = "storage.azure.account.key";
    public static final String CONF_BUCKET = "storage.azure.bucket";

    private final static String CONNECTION_PATTERN = "DefaultEndpointsProtocol={0};AccountName={1};AccountKey={2};";
    private final static String URL_PATTERN = "http://{0}.blob.core.windows.net/{1}/{2}";

    private CloudBlobClient blobClient;
    private CloudBlobContainer blobContainer;
    private String accountName;

    public AzureService(Map<String, String> conf) {
        super(conf, AzureObject.class);
    }

    @Override
    protected void configure(Map<String, String> conf) {
        super.configure(conf, "azure");
        String protocol = getConfValue(conf, CONF_PROTOCOL, "");
        String accountKey = getConfValue(conf, CONF_ACCOUNT_KEY, "");
        this.accountName = getConfValue(conf, CONF_ACCOUNT_NAME, "");
        connect(protocol, accountName, accountKey, conf.get(CONF_BUCKET));
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
            throw new ConfigurationException(exception);
        }
    }

    @Override
    protected void doRemove(String fullPath) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(fullPath);
            blob.deleteIfExists();
        } catch (Exception e) {
            throw E.unexpected(e);
        }
    }

    @Override
    public String getUrl(String key) {
        return MessageFormat.format(URL_PATTERN, accountName, blobContainer.getName(), keyWithContextPath(key));
    }

    @Override
    protected String keyWithContextPath(String key) {
        String fullPath = super.keyWithContextPath(key);
        if (fullPath.startsWith("/")) {
            fullPath = fullPath.substring(1);
        }
        return fullPath;
    }

    @Override
    protected Map<String, String> doGetMeta(String fullPath) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(fullPath);
            blob.downloadAttributes();
            Map<String, String> meta = C.newMap(blob.getMetadata());
            meta.put(ISObject.ATTR_CONTENT_TYPE, blob.getProperties().getContentType());
            return meta;
        } catch (Exception e) {
            throw E.unexpected(e);
        }
    }

    @Override
    protected ISObject newSObject(String key) {
        return new AzureObject(key, this);
    }

    @Override
    protected void doPut(String fullPath, ISObject stuff, Map<String, String> attrs) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(fullPath);
            if (!(stuff instanceof SObject.InputStreamSObject)) {
                blob.upload(stuff.asInputStream(), stuff.getLength());
            }
            BlobProperties props = blob.getProperties();
            // content-type contains "-" which is illegal character in C# identifier
            // so we have to remove it from meta map
            String contentType = attrs.remove(ISObject.ATTR_CONTENT_TYPE);
            if (S.notBlank(contentType)) {
                props.setContentType(contentType);
            }
            blob.uploadProperties();
            blob.getMetadata().putAll(attrs);
            blob.uploadMetadata();
        } catch (Exception e) {
            throw E.unexpected(e);
        }
    }

    @Override
    protected StorageServiceBase newService(Map<String, String> conf) {
        return new AzureService(conf);
    }

    @Override
    protected InputStream doGetInputStream(String fullPath) {
        try {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(fullPath);
            return blob.openInputStream();
        } catch (Exception exception) {
            throw E.unexpected(exception);
        }
    }
}
