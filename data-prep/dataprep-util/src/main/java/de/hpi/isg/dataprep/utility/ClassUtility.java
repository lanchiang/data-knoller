package de.hpi.isg.dataprep.utility;

import java.util.LinkedList;
import java.util.List;

/**
 * @author lan.jiang
 * @since 1/16/19
 */
public class ClassUtility {

    /**
     * Check whether the classes given by the parameter classNames all exist, given the package path.
     *
     * @param classNames
     * @param packagePath
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
//                throw new ClassNotFoundException("Class " + className + " cannot be found.");
                missingClasses.add(className);
            }
        }
        if (missingClasses.size()>0) {
            throw new RuntimeException("Classes " + missingClasses.toString() + " cannot be found.");
        }
    }

    public static String getPackagePath(String className, String packagePath) {
        if (!packagePath.endsWith(".")) {
            packagePath += ".";
        }
        return packagePath + className;
    }
}
