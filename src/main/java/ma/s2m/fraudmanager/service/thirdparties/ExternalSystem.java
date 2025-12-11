package ma.s2m.fraudmanager.service.thirdparties;

public class ExternalSystem implements IExternalSystem<String, Boolean> {

    @Override
    public Boolean getInformation(java.lang.String infoType, String object) {
                if (infoType != null && infoType.equals("countryRisky") && (object.equals("FR") || object.equals("US")))
            return Boolean.FALSE;
        else {
            return Boolean.TRUE;
        }
    }
}
