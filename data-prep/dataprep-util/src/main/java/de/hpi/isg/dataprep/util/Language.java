package de.hpi.isg.dataprep.util;

import java.io.Serializable;

/**
 * @author lan.jiang
 * @since 1/26/19
 */
public enum Language {
    EN("English"),
    DE("Deutsch");

    private String language;

    Language(String language) {
        this.language = language;
    }

    public String getLanguage() {
        return language;
    }
}
