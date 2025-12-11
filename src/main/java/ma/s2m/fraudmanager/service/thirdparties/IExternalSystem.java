package ma.s2m.fraudmanager.service.thirdparties;

public interface IExternalSystem<INPUT,OUTPUT> {
    public OUTPUT getInformation(String infoType, INPUT object);
}
