package software.aws.rds.jdbc.proxydriver;

public interface HostListProviderService {

    HostListProvider getHostListProvider();
    boolean isDefaultHostListProvider();

    void setHostListProvider(HostListProvider hostListProvider);
}
