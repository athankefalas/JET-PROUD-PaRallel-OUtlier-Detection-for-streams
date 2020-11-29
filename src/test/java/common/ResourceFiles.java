package common;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public final class ResourceFiles {

    private ResourceFiles(){}

    public static String absolutePathOf(String path) {
        try {
            // get the file url, not working in JAR file.
            URL resource = ResourceFiles.class.getClassLoader().getResource(path);

            if (resource == null) {
                throw new IOException("File not found at path.");
            } else {
                return resource.toURI().toString()
                        .replace("file:/", "")
                        .replace("/", File.separator)
                        .replace("%20", " ");
            }
        } catch (URISyntaxException | IOException exception) {
            return null;
        }
    }

    public static File fromPath(String path) {
        try {
            // get the file url, not working in JAR file.
            URL resource = ResourceFiles.class.getClassLoader().getResource(path);

            if (resource == null) {
                throw new IOException("File not found at path.");
            } else {
                File file = new File(resource.toURI());

                if (!file.exists() || !file.isFile()) {
                    throw new IOException("File does not exist.");
                }

                return file;
            }
        } catch (URISyntaxException | IOException exception) {
            return null;
        }
    }


}
