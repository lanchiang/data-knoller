package de.hpi.isg.dataprep.utility;

import java.util.LinkedList;
import java.util.List;

/**
 * This class provides {@link Class} utility functions.
 *
 * @author lan.jiang
 * @since 1/16/19
 */
public class ClassUtility {

    /**
     * Check whether the classes given by the parameter classNames all exist, given the package path.
     *
     * @param classNames is the names of the classes to be checked
     * @param packagePath is the package path
     */
    public static void checkClassesExistence(String[] classNames, String packagePath) {
        if (!packagePath.endsWith(".")) {
            packagePath += ".";
        }
        List<String> missingClasses = new LinkedList<>();
        for (String className : classNames) {
            String absoluteClassName = packagePath + className;
            try {
                Class.forName(absoluteClassName);
            } catch (ClassNotFoundException e) {
                missingClasses.add(className);
            }
        }
        if (missingClasses.size()>0) {
            throw new RuntimeException("Classes " + missingClasses.toString() + " cannot be found.");
        }
    }

    /**
     * Get the whole path of the given class.
     *
     * @param className is the name of the class
     * @param packagePath is the package path.
     * @return the full path of the given class
     */
    public static String getClassFullPath(String className, String packagePath) {
        if (!packagePath.endsWith(".")) {
            packagePath += ".";
        }
        return packagePath + className;
    }
}
