package storm;

import backtype.storm.nimbus.INimbusStorage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Module;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author slukjanov
 */
public class SwiftNimbusStorage implements INimbusStorage {

    private static final Set<? extends Module> MODULES = ImmutableSet.of(new SLF4JLoggingModule());
    private static final ListContainerOptions LIST_CONTAINER_OPTIONS = ListContainerOptions.Builder.recursive();

    public static final String NIMBUS_STORAGE_ATTR_PREFIX = "nimbus.storage.swift.";
    public static final String NIMBUS_STORAGE_SWIFT_AUTH = NIMBUS_STORAGE_ATTR_PREFIX + "auth";
    public static final String NIMBUS_STORAGE_SWIFT_IDENTITY = NIMBUS_STORAGE_ATTR_PREFIX + "identity";
    public static final String NIMBUS_STORAGE_SWIFT_PASSWORD = NIMBUS_STORAGE_ATTR_PREFIX + "password";
    public static final String NIMBUS_STORAGE_SWIFT_CONTAINER = NIMBUS_STORAGE_ATTR_PREFIX + "container";
    public static final String NIMBUS_STORAGE_SWIFT_TIMEOUT = NIMBUS_STORAGE_ATTR_PREFIX + "timeout";

    public static final int DEFAULT_TIMEOUT = 30000;
    public static final String DEFAULT_CONTAINER = "storm-ha";

    private AsyncBlobStore store;
    private String container;

    @Override
    public void init(Map config) {
        try {
            String endpoint = (String) config.get(NIMBUS_STORAGE_SWIFT_AUTH);
            String identity = (String) config.get(NIMBUS_STORAGE_SWIFT_IDENTITY);
            String password = (String) config.get(NIMBUS_STORAGE_SWIFT_PASSWORD);

            container = (String) config.get(NIMBUS_STORAGE_SWIFT_CONTAINER);
            if (container == null) {
                container = DEFAULT_CONTAINER;
            }

            String timeoutStr = (String) config.get(NIMBUS_STORAGE_SWIFT_TIMEOUT);
            int timeout = DEFAULT_TIMEOUT;
            if (timeoutStr != null) {
                timeout = Integer.parseInt(timeoutStr);
            }

            BlobStoreContext context = ContextBuilder.newBuilder("swift-keystone")
                    .endpoint(endpoint)
                    .credentials(identity, password)
                    .modules(MODULES)
                    .buildView(BlobStoreContext.class);

            store = context.getAsyncBlobStore();

            if (!store.containerExists(container).get(timeout, TimeUnit.MILLISECONDS)) {
                if (!store.createContainerInLocation(null, container).get(timeout, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Can't create specified container");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while initializing Swift client", e);
        }
    }

    @Override
    public InputStream open(String path) {
        try {
            return store.getBlob(container, path).get().getPayload().getInput();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputStream create(String path) {
        try {
            PipedOutputStream out = new PipedOutputStream();
            PipedInputStream in = new PipedInputStream();
            out.connect(in);
            Blob blob = store.blobBuilder(path).payload(in).build();
            store.putBlob(container, blob); // don't wait

            return out;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> list(String path) {
        try {
            path = path.endsWith("/") ? path : path + "/";
            Set<String> result = Sets.newTreeSet();
            PageSet<? extends StorageMetadata> list = store.list(container, LIST_CONTAINER_OPTIONS).get();
            for (StorageMetadata metadata : list) {
                String name = metadata.getName();
                if (name.startsWith(path)) {
                    name = name.substring(path.length());
                    int idx = name.indexOf('/');
                    if (idx >= 0) {
                        name = name.substring(0, idx);
                    }
                    result.add(name);
                }
            }

            return Lists.newArrayList(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        store.removeBlob(container, path);
    }

    @Override
    public void mkdirs(String path) {
        try {
            store.createDirectory(container, path).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isSupportDistributed() {
        return true;
    }

}
