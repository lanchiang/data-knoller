package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import scala.util.matching.Regex;

public class DINPhoneNumber extends Metadata {

    private Boolean countryCode;
    private Boolean areaCode;
    private Boolean specialNumber;
    private Boolean extensionNumber;
    private Regex regex;

    private DINPhoneNumber() {
        super(DINPhoneNumber.class.getSimpleName());
    }

    public DINPhoneNumber(Boolean countryCode, Boolean areaCode, Boolean specialNumber, Boolean extensionNumber, Regex regex) {
        this();
        this.countryCode = countryCode;
        this.areaCode = areaCode;
        this.specialNumber = specialNumber;
        this.extensionNumber = extensionNumber;
        this.regex = regex;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public Boolean getCountryCode() {
        return countryCode;
    }

    public Boolean getAreaCode() {
        return areaCode;
    }

    public Boolean getSpecialNumber() {
        return specialNumber;
    }

    public Boolean getExtensionNumber() {
        return extensionNumber;
    }

    public Regex getRegex() {
        return regex;
    }

}
