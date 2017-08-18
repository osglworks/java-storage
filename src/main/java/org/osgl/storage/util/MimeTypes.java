package org.osgl.storage.util;

import org.osgl.util.C;
import org.osgl.util.IO;
import org.osgl.util.S;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.util.Map;
import java.util.Properties;

public class MimeTypes {
    // some common types that are missing from java activation utils
    private static Map<String, String> commonMimeTypes = C.map(
            "pdf", "application/pdf",
            "png", "image/png",
            "bmp", "image/bmp"
    );
    private static MimetypesFileTypeMap activationMimeTypes = new MimetypesFileTypeMap();
    private static Properties mimeTypes;
    static {
        // TODO - use osgl-tool built in mime types when update to osgl-tool-2.x
        final String OSGL_HTTP_MIME_TYPES = "/org/osgl/http/mime-types.properties";
        try {
            mimeTypes = IO.loadProperties(
                    IO.is(MimeTypes.class.getResource(OSGL_HTTP_MIME_TYPES)));
        } catch (Exception e) {
            mimeTypes = new Properties();
        }
    }

    public static String mimeType(File file) {
        return mimeType(file.getName());
    }

    public static String mimeType(String fileName) {
        String suffix = S.afterLast(fileName, ".");
        String mimeType = commonMimeTypes.get(suffix);
        if (null != mimeType) {
            return mimeType;
        }
        mimeType = mimeTypes.getProperty(S.afterLast(fileName, "."));
        return null != mimeType ? mimeType : activationMimeTypes.getContentType(fileName);
    }
}
