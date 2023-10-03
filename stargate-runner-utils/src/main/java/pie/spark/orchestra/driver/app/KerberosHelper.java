package pie.spark.orchestra.driver.app;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class KerberosHelper {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public Optional<String> getKerberosProxyUserOptional(final UserGroupInformation currentUgi) throws Exception {
        LOGGER.debug("KerberosHelper is disabled by Stargate. Will return getKerberosProxyUserOptional as Optional.empty");
        return Optional.empty();
    }

    public UserGroupInformation getKerberosProxyUserUgi(final String kerberosProxyUser, final UserGroupInformation currentUgi) {
        LOGGER.debug("KerberosHelper is disabled by Stargate. getKerberosProxyUserUgi invoked");
        return UserGroupInformation.createProxyUser(kerberosProxyUser, currentUgi);
    }

    public UserGroupInformation getCurrentUgi() throws IOException {
        LOGGER.debug("KerberosHelper is disabled by Stargate. Will return getCurrentUgi == null");
        return null;
    }
}
