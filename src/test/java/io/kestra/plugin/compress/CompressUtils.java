package io.kestra.plugin.compress;

import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

@Singleton
class CompressUtils {
    @Inject
    private StorageInterface storageInterface;

    URI uploadToStorageString(String content) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new ByteArrayInputStream(content.getBytes())
        );
    }

    URI uploadToStorage(String resource) throws Exception {
        File applicationFile = new File(Objects.requireNonNull(CompressUtils.class.getClassLoader()
            .getResource(resource))
            .toURI()
        );

        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(applicationFile)
        );
    }
}
